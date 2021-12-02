package com.cookpad.prism.dao;

import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface PrismTableMapper {
    public PrismTable find(@Param("tableId") int tableId);
    public OneToMany<PrismTable, StreamColumn> findWithColumns(@Param("tableId") int tableId);
    public List<OneToMany<PrismTable, StreamColumn>> getAllWithColumns();
    public void unlink(@Param("tableId") int tableId);
    public void drop(@Param("tableId") int tableId);
}
