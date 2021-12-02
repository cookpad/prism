package com.cookpad.prism.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.cookpad.prism.TempFile;
import com.cookpad.prism.record.Schema;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ParallelParquetMerger {
    private final ExecutorService downloadExecutor;
    private final ExecutorService mergeExecutor;
    private final ParquetFileMerger parquetFileMerger;

    public void shutdown() {
        downloadExecutor.shutdownNow();
        try {
            while (! downloadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("waiting for downloadExecutor shutdown...");
            }
        }
        catch (InterruptedException ex) {
            log.error("downloadExecutor shutdown interrupted: {}", ex.getMessage());
        }

        mergeExecutor.shutdownNow();
        try {
            while (! mergeExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("waiting for mergeExecutor shutdown...");
            }
        }
        catch (InterruptedException ex) {
            log.error("mergeExecutor shutdown interrupted: {}", ex.getMessage());
        }
    }

    public TempFile merge(Schema schema, List<? extends Supplier<? extends TempFile>> suppliers) {
        Node root = this.buildTree(suppliers);
        var result = this.mergeTree(schema, root);
        logVMMemoryUsage();
        return result;
    }

    public TempFile mergeTree(Schema schema, Node root) {
        var result = root.toCompletableFuture(schema).join();
        logVMMemoryUsage();
        return result;
    }

    public Node toNode(Supplier<? extends TempFile> supplier) {
        return new SupplierNode(this.downloadExecutor, supplier);
    }

    public Node toMergeNode(Node left, Node right) {
        return new MergeNode(this.mergeExecutor, this.parquetFileMerger, left, right);
    }

    public Node buildTree(List<? extends Supplier<? extends TempFile>> suppliers) {
        List<Node> supplierNodes = suppliers.stream()
                .map((supplier) -> this.toNode(supplier)).collect(Collectors.toList());
        return this.buildTreeFromNodeList(supplierNodes);
    }

    private Node buildTreeFromNodeList(List<Node> nodes) {
        if (nodes.size() == 1) {
            return nodes.get(0);
        }
        List<Node> newNodes = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i += 2) {
            if (i + 1 < nodes.size()) {
                Node left = nodes.get(i);
                Node right = nodes.get(i + 1);
                Node newNode = this.toMergeNode(left, right);
                newNodes.add(newNode);
            } else {
                newNodes.add(nodes.get(i));
            }
        }
        return this.buildTreeFromNodeList(newNodes);
    }

    void logVMMemoryUsage() {
        var rt = Runtime.getRuntime();
        long free = rt.freeMemory();
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        log.info("vm memory usage: used {}, free {}, total {}, max {}", total - free, free, total, max);
    }

    @RequiredArgsConstructor
    @Slf4j
    public static class MergeTask implements BiFunction<TempFile, TempFile, TempFile> {
        private final Schema schema;
        private final ParquetFileMerger parquetFileMerger;

        @Override
        public TempFile apply(TempFile t, TempFile u) {
            TempFile outFile = null;
            boolean succeeded = false;
            try {
                outFile = new TempFile("prism-merge-", ".parquet");
                this.parquetFileMerger.merge(schema, t.getPath(), u.getPath(), outFile.getPath());
                succeeded = true;
                return outFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                closeForce(t);
                closeForce(u);
                if (outFile != null && !succeeded) {
                    closeForce(outFile);
                }
            }
        }

        void closeForce(TempFile f) {
            try {
                f.close();
            } catch (IOException e) {
                log.error("IO exception on deleting temporary file: " + e.getMessage());
            }
        }
    }

    public static interface Node {
        public CompletableFuture<? extends TempFile> toCompletableFuture(Schema schema);
    }

    @RequiredArgsConstructor
    private static class SupplierNode implements Node {
        private final ExecutorService executor;
        private final Supplier<? extends TempFile> supplier;

        @Override
        public CompletableFuture<? extends TempFile> toCompletableFuture(Schema _schema) {
            return CompletableFuture.supplyAsync(this.supplier, this.executor);
        }
    }

    @RequiredArgsConstructor
    private static class MergeNode implements Node {
        private final ExecutorService executor;
        private final ParquetFileMerger parquetFileMerger;

        @Getter
        @Setter
        @NonNull
        private Node left;
        @Getter
        @Setter
        @NonNull
        private Node right;

        @Override
        public CompletableFuture<TempFile> toCompletableFuture(Schema schema) {
            return this.left.toCompletableFuture(schema).thenCombineAsync(this.right.toCompletableFuture(schema),
                    new MergeTask(schema, this.parquetFileMerger), this.executor);
        }
    }
}
