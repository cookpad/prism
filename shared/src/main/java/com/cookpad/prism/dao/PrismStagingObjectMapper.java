package com.cookpad.prism.dao;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

public interface PrismStagingObjectMapper {
    @Select("select * from prism_staging_objects where object_key = #{objectKey} and bucket_name = #{bucketName}")
    @ResultMap("prismStagingObjectMap")
    PrismStagingObject findByBucketNameAndObjectKey(@Param("bucketName") String bucketName, @Param("objectKey") String objectKey);

    @Insert("insert into prism_staging_objects(bucket_name, object_key, send_time, first_receive_time) values (#{bucketName}, #{objectKey}, #{sendTime}, #{firstReceiveTime})")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "prism_staging_object_id")
    void create(PrismStagingObject newObject);
}
