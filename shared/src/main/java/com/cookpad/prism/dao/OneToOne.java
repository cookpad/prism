package com.cookpad.prism.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OneToOne<L, R> {
    private L left;
    private R right;
}
