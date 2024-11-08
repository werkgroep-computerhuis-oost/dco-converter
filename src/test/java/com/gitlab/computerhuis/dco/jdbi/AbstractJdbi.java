package com.gitlab.computerhuis.dco.jdbi;

import lombok.NonNull;
import lombok.val;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Map;

abstract class AbstractJdbi {

    protected Jdbi jdbi;
    protected Handle handle;

    public boolean exist(@NonNull final String table, @NonNull final String column, @NonNull final Object value) {
        val sql = "SELECT TRUE AS exist FROM %s WHERE %s=:value".formatted(table, column);
        val found = handle.createQuery(sql).bind("value", value).mapTo(Boolean.class).findOne();
        return found.orElse(false);
    }

    public String create_sql(@NonNull final String table, @NonNull final Map<String, Object> data) {
        return "INSERT INTO %s (%s) VALUES (:%s);".formatted(table, String.join(",", data.keySet()), String.join(",:", data.keySet()));
    }

    public void insert(@NonNull final String table, @NonNull final Map<String, Object> row) {
        val sql = create_sql(table, row);
        handle.createUpdate(sql).bindMap(row).execute();
    }

    public List<Map<String, Object>> select(@NonNull final String sql) {
        return handle.createQuery(sql).mapToMap().list();
    }

    public Handle getHandle() {
        return handle;
    }

    public void open() {
        handle = jdbi.open();
    }

    public void close() {
        handle.close();
    }
}
