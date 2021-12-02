package com.cookpad.prism.dao;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

public interface PrismUnknownStagingObjectMapper {
    @Select("select * from prism_unknown_staging_objects where object_key = #{objectKey} and bucket_name = #{bucketName}")
    @ResultMap("prismUnknownStagingObjectMap")
    PrismUnknownStagingObject findByBucketNameAndObjectKey(@Param("bucketName") String bucketName, @Param("objectKey") String objectKey);

    @Insert("insert into prism_unknown_staging_objects(bucket_name, object_key, send_time, first_receive_time, message) values (#{bucketName}, #{objectKey}, #{sendTime}, #{firstReceiveTime}, #{message})")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "prism_unknown_staging_object_id")
    void create(PrismUnknownStagingObject newObject);
}
