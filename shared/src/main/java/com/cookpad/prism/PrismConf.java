package com.cookpad.prism;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.NoArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ConfigurationProperties(prefix="prism")
@NoArgsConstructor
@Getter
@Setter
@ToString
public class PrismConf {
    String bucketName;
    String prefix;
}
