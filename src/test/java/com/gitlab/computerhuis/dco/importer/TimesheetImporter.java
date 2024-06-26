package com.gitlab.computerhuis.dco.importer;

import com.gitlab.computerhuis.dco.jdbi.AccessJdbi;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TimesheetImporter {

    private final AccessJdbi accessJdbi;
    private final MariadbJdbi mariadbJdbi;

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing timesheets from access");
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT * FROM Tbl_Tijdsregistratie WHERE Datum > #%s# and Activiteit is not null".formatted(importDateFrom));
        for (val timesheet : rows) {
            Integer activity_id = lookupActiviteitNr(String.valueOf(timesheet.get("activiteit")));

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (timesheet.get("datum") != null && timesheet.get("datum") instanceof Timestamp) {
                registered = ((Timestamp) timesheet.get("datum")).toLocalDateTime();
            }

            Integer person_id = (Integer) timesheet.get("gebruikersnummer");

            if (mariadbJdbi.exist("individuals", "id", person_id) && !doesTimesheetExist(person_id, activity_id, registered)) {
                Map<String, Object> row = new HashMap<>();
                row.put("person_id", person_id);
                row.put("registered", registered);
                row.put("activity_id", activity_id);
                row.put("unregistered", registered);

                mariadbJdbi.insert("timesheets", row);
            }
        }
    }

    private boolean doesTimesheetExist(final Integer person_id, final Integer activity_id, final LocalDateTime registered) throws Exception {
        val sql = "SELECT TRUE AS exist FROM timesheets WHERE person_id=:person_id AND activity_id=:activity_id AND registered=:registered";
        val found = mariadbJdbi.getJdbi().withHandle(handle -> handle.createQuery(sql)
            .bind("person_id", person_id)
            .bind("activity_id", activity_id)
            .bind("registered", registered)
            .mapTo(Boolean.class).findOne());
        return found.orElse(false);
    }

    private Integer lookupActiviteitNr(@NonNull final String activiteit) throws Exception {
        return switch (activiteit.toUpperCase()) {
            case "CURSUSSEN" -> 1;
            case "LESOPMAAT" -> 2;
            case "MEDEWERKER" -> 3;
            case "MEDEWERKER WERKPLAATS" -> 4;
            case "ONLINE BEGELEIDING" -> 5;
            case "TAALONDERSTEUNING" -> 6;
            case "VRIJE INLOOP" -> 7;
            case "WERKPLAATS" -> 8;
            case "WORKSHOPS" -> 9;
            case "VLINDERTUIN" -> 10;
            default -> throw new IllegalStateException("Unexpected value: " + activiteit.toUpperCase());
        };
    }
}
