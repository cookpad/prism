package com.cookpad.prism.stream;

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
@ToString(callSuper = true)
public class PrismStreamConf extends PrismConf {
    String queueUrl;
    String ignoreToInclusive;
    String ignoreFromExclusive;
}
