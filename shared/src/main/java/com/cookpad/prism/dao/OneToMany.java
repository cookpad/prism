package com.cookpad.prism.dao;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OneToMany<O, M> {
    private O one;
    private List<M> many;
}
