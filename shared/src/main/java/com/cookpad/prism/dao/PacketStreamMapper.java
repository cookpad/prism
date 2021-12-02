package com.cookpad.prism.dao;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface PacketStreamMapper {
    List<OneToMany<PacketStream, StreamColumn>> findByStreamName(@Param("streamName") String streamName);

    List<OneToOne<OneToMany<PacketStream, StreamColumn>, PrismTable>> findByDestBucketAndPrefix(
            @Param("destBucket") String destBucket, @Param("destPrefix") String destPrefix);

    List<OneToMany<PacketStream, StreamColumn>> findByPrismTableId(@Param("tableId") int tableId);
}
