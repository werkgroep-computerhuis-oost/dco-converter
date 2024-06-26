package com.gitlab.computerhuis.dco.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
import com.gitlab.computerhuis.dco.util.PathTestUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class ActivitiesImporter {

    private final MariadbJdbi mariadbJdbi;
    private final ObjectMapper mapper;

    public void importFromJson() throws Exception {
        log.info("Start importing activities from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/activities.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("activities", "id", row.get("id"))) {
                mariadbJdbi.insert("activities", row);
            }
        }
    }
}
