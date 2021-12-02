package com.cookpad.prism.batch;

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
public class PrismBatchConf extends PrismConf {
    Catalog catalog = new Catalog();

    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class Catalog {
        String databasePrefix = "";
        String databaseSuffix = "";
    }
}
