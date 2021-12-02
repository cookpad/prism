package com.cookpad.prism.stream.events;

import java.util.List;

import com.cookpad.prism.stream.StagingObjectAttributes;
import com.cookpad.prism.stream.StagingObjectAttributes.NotAnStagingObjectException;
import com.cookpad.prism.stream.events.StagingObjectHandler.UnknownObjectException;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.OneToOne;
import com.cookpad.prism.dao.PacketStream;
import com.cookpad.prism.dao.PacketStreamMapper;
import com.cookpad.prism.dao.PrismStagingObject;
import com.cookpad.prism.dao.PrismStagingObjectMapper;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismUnknownStagingObject;
import com.cookpad.prism.dao.PrismUnknownStagingObjectMapper;
import com.cookpad.prism.dao.StreamColumn;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StagingObjectDispatcher implements EventHandler {
    final private StagingObjectHandler handler;
    final private PrismStagingObjectMapper stagingObjectMapper;
    final private PrismUnknownStagingObjectMapper unknownStagingObjectMapper;
    private final PacketStreamMapper packetStreamMapper;
    private final DateRange ignoreDateRange;

    @Transactional(propagation=Propagation.NESTED)
    private PrismStagingObject findOrCreateStagingObject(StagingObjectEvent event, StagingObjectAttributes attrs) {
        PrismStagingObject stagingObject = this.stagingObjectMapper.findByBucketNameAndObjectKey(event.getBucketName(), event.getObjectKey());
        if (stagingObject != null) {
            log.debug("Object is already in table: {}", stagingObject);
            return stagingObject;
        }
        PrismStagingObject newStagingObject = event.toStagingObject();
        this.stagingObjectMapper.create(newStagingObject);
        log.debug("Object is newly created in table: {}", newStagingObject);
        return newStagingObject;
    }

    @Transactional(propagation=Propagation.NESTED)
    private void createUnknownStagingObjectIfNotExist(StagingObjectEvent event, String message) {
        PrismUnknownStagingObject unknownStagingObject = this.unknownStagingObjectMapper.findByBucketNameAndObjectKey(event.getBucketName(), event.getObjectKey());
        if (unknownStagingObject != null) {
            log.debug("Unknown object is already recorded: {}", unknownStagingObject);
            return;
        }
        PrismUnknownStagingObject newUnknownStagingObject = event.toUnknownStagingObject(message);
        this.unknownStagingObjectMapper.create(newUnknownStagingObject);
        log.debug("Unknown object is newly recorded in table: {}", newUnknownStagingObject);
    }

    @Override
    public void handleEvent(StagingObjectEvent event) throws CatchAndReleaseException {
        log.debug("Handle Event: {}", event);
        final StagingObjectAttributes attrs;
        try {
            attrs = StagingObjectAttributes.parse(event.getObjectKey());
        } catch (NotAnStagingObjectException e) {
            this.raiseNotAnInputObjectError(event);
            return;
        }
        if (ignoreDateRange.contains(attrs.getDate())) {
            log.debug("In ignore date range: event={}, attrs={}", attrs);
            throw new EventHandler.CatchAndReleaseException();
        }
        log.debug("Object Attrs: {}", attrs);

        List<OneToOne<OneToMany<PacketStream, StreamColumn>, PrismTable>> relation = this.packetStreamMapper.findByDestBucketAndPrefix(event.getBucketName(), attrs.getStreamPrefix());
        if (relation.size() == 0) {
            this.raiseNoStreamError(event);
            return;
        }
        if (relation.size() != 1) {
            this.raiseMultipleStreamError(event);
            return;
        }
        OneToOne<OneToMany<PacketStream, StreamColumn>, PrismTable> firstOne = relation.get(0);
        OneToMany<PacketStream, StreamColumn> packetStreamWithColumns = firstOne.getLeft();
        PrismTable table = firstOne.getRight();

        PrismStagingObject stagingObject = this.findOrCreateStagingObject(event, attrs);
        PacketStream packetStream = packetStreamWithColumns.getOne();
        if (packetStream.isDisabled() || packetStream.isDiscard() || !packetStream.isInitialized()) {
            log.info("Ignore staging object: {}: {}", packetStream, stagingObject);
            return;
        }
        try {
            this.handler.handleStagingObject(stagingObject, attrs, packetStreamWithColumns, table);
        } catch (UnknownObjectException ex) {
            this.raiseBadEventError(event, ex);
        } catch (Exception ex) {
            throw new StagingObjectHandlingException(stagingObject, ex);
        }
    }

    private void raiseNotAnInputObjectError(StagingObjectEvent event) {
        log.warn("Not an input object: {}", event.getObjectUri().toString());
        this.createUnknownStagingObjectIfNotExist(event, "not an input object");
    }

    private void raiseNoStreamError(StagingObjectEvent event) {
        log.error("No stream for: {}", event.getObjectUri().toString());
        this.createUnknownStagingObjectIfNotExist(event, "no stream");
    }

    private void raiseMultipleStreamError(StagingObjectEvent event) {
        log.error("Data integrity error: Multiple streams are matched for: {}", event.getObjectUri().toString());
        this.createUnknownStagingObjectIfNotExist(event, "multiple stream");
    }

    private void raiseBadEventError(StagingObjectEvent event, UnknownObjectException ex) {
        log.error("Forwarded the bad event to unknown staging objects: {}", event, ex);
        this.createUnknownStagingObjectIfNotExist(event, "bad event");
    }

    @SuppressWarnings("serial")
    public static class StagingObjectHandlingException extends RuntimeException {
        StagingObjectHandlingException(PrismStagingObject stagingObject, Exception cause) {
            super("StagingObject: " + stagingObject.toString(), cause);
        }
    }
}
