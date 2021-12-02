package com.cookpad.prism.merge;

import com.cookpad.prism.PrismConf;

import org.springframework.stereotype.Component;

import lombok.NoArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Component
@NoArgsConstructor
@Getter
@Setter
@ToString
public class PrismMergeConf extends PrismConf {
    long mergeJobTimeout;
    long mergedObjectSize;
    int mergeBatchSize;
    int downloaderThreads;
    int mergerThreads;
}
