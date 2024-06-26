package com.gitlab.computerhuis.dco.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gitlab.computerhuis.dco.jdbi.AccessJdbi;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
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
public class TicketImporter {

    private static final Map<String, Integer> LOOKUP_NAME = new HashMap<>() {{
        // Joris Pierre Kleijnen
        put("joris", 1937);
        put("Joris", 1937);

        // Frans van der Meijden
        put("Frans", 1544);
        put("frans", 1544);
        put("Frans/Ali", 1544);
        put("Frans/Jan", 1544);
        put("Frans/Antonie", 1544);
        put("Frans/Ary", 1544);
        put("Frans/Sjef", 1544);

        // Ary Safari - Al Baldawi
        put("Ali", 1648);
        put("Ali & Stephan", 1648);
        put("Ali en Bas", 1648);
        put("Ali/Frans", 1648);
        put("Ali/Jan", 1648);
        put("Ali/Stephan", 1648);
        put("joris en ali", 1648);
        put("Ale", 1648);
        put("adi", 1648);
        put("Ary", 1648);
        put("Ari", 1648);
        put("Ary/Frans", 1648);
        put("Ary/Antonie", 1648);
        put("ary", 1648);
        put("Adi", 1648);

        // Henri Dona
        put("henri", 1804);
        put("Hen ri", 1804);
        put("Henry", 1804);
        put("Henri", 1804);

        // Peter Ruyters
        put("Peter", 740);

        // Sjef Lievens
        put("Sjef", 897);
        put("swjef", 897);
        put("sjef", 897);
        put("Sjef/Frans", 897);
        put("Sjef en Frans", 897);
        put("sjf", 897);

        // Thomas Carpentier
        put("Thomas", 1839);
        put("Thomas & Frans", 1839);
        put("Thomas/Frans", 1839);

        // Jan van der Pol
        put("Jan", 1408);
        put("Jan/Frans", 1408);
        put("JAN/Frans", 1408);
        put("Jan van de Pol", 1408);

        // Tim Voogt
        put("Tim Voogt", 2097);
        put("Tim", 2097);

        // Sander Stumpel
        put("Sander", 2106);
        put("sander", 2106);

        // Willie Voets
        put("Willie/Frans", 2174);
        put("Willie-Frans", 2174);
        put("Wil-Frans", 2174);
        put("Willie", 2174);
        put("Willie Voets", 2174);
        put("willie", 2174);

        // Wil Verberne
        put("Wil", 1284);

        // Antonie Gelderblom
        put("Antonie", 2268);
        put("Antonie/Frans", 2268);
    }};

    private final AccessJdbi accessJdbi;
    private final MariadbJdbi mariadbJdbi;
    private final ObjectMapper mapper;
    private final EquipmentImporter equipmentImporter;
    private final IndividualsImporter individualsImporter;

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing tickets from access");
        final List<Map<String, Object>> tickets = accessJdbi.select("SELECT * FROM Tbl_Reparaties_main WHERE [datum inname] > #%s#".formatted(importDateFrom));
        for (val ticket : tickets) {
            if (ticket != null && !ticket.isEmpty() && !mariadbJdbi.exist("tickets", "id", ticket.get("reparatienummer"))) {
                String ticket_type = "REPAIR";
                if (String.valueOf(ticket.get("Probleem")).toLowerCase().contains("uitgifte")) {
                    ticket_type = "ISSUE";
                }

                LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
                if (ticket.get("datum inname") != null && ticket.get("datum inname") instanceof Timestamp) {
                    registered = ((Timestamp) ticket.get("datum inname")).toLocalDateTime();
                }

                Map<String, Object> row = new HashMap<>();
                row.put("id", ticket.get("reparatienummer"));
                row.put("ticket_type", ticket_type);
                row.put("registered", registered);
                row.put("equipment_id", ticket.get("computernummer"));

                Map<String, String> description = new HashMap<>();
                description.put("probleem", (String) ticket.get("probleem"));
                description.put("backup", (String) ticket.get("backup"));
                description.put("meegeleverde_accessoires", (String) ticket.get("bijgeleverde accessoires"));
                description.put("samenvatting", (String) ticket.get("samenvatting reparatie"));

                row.put("description", mapper.writeValueAsString(description));

                equipmentImporter.importFromAcces((Integer) ticket.get("computernummer"), importDateTimeFrom, importDateTimeFrom);
                mariadbJdbi.insert("tickets", row);

                importTicketStatusAndLog(ticket, importDateTimeFrom);
                importLogs((Integer) ticket.get("reparatienummer"), importDateTimeFrom);
            }
        }
    }


    private void importTicketStatusAndLog(final Map<String, Object> row, final String importDateTimeFrom) throws Exception {
        if (row != null && !row.isEmpty()) {
            Integer personId = LOOKUP_NAME.get(row.get("aangenomen door"));
            if (personId == null) {
                personId = 1;
            } else {
                individualsImporter.importFromAccess(personId, importDateTimeFrom);
            }

            Integer uitgevoerdDoor = LOOKUP_NAME.get(row.get("medewerker"));
            if (uitgevoerdDoor == null) {
                uitgevoerdDoor = 1;
            } else {
                individualsImporter.importFromAccess(uitgevoerdDoor, importDateTimeFrom);
            }

            mariadbJdbi.insert("ticket_status", Map.of(
                "ticket_id", row.get("reparatienummer"),
                "date", row.get("datum inname"),
                "volunteer_id", personId,
                "status", "OPEN"
            ));

            // --[ READY ]----------------------------------------------------------------------------------------------
            if (row.containsKey("datum opgelost") && row.get("datum opgelost") != null) {
                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", String.valueOf(row.get("datum opgelost")).substring(0, 10) + " 01:00:00",
                    "volunteer_id", uitgevoerdDoor,
                    "status", "READY"
                ));
            }

            // --[ CUSTOMER_CALLED ]------------------------------------------------------------------------------------
            boolean ready = false;
            if (row.containsKey("datum gebeld3") && row.get("datum gebeld3") != null) {
                ready = true;

                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", String.valueOf(row.get("datum gebeld3")).substring(0, 10) + " 18:00:00",
                    "volunteer_id", uitgevoerdDoor,
                    "status", "CUSTOMER_INFORMED"
                ));

                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", String.valueOf(row.get("datum gebeld3")).substring(0, 10) + " 18:00:00",
                    "volunteer_id", uitgevoerdDoor,
                    "log", row.get("reactie gebeld3")
                ));
            }

            if (row.containsKey("datum gebeld2") && row.get("datum gebeld2") != null) {
                if (!ready) {
                    ready = true;

                    mariadbJdbi.insert("ticket_status", Map.of(
                        "ticket_id", row.get("reparatienummer"),
                        "date", String.valueOf(row.get("datum gebeld2")).substring(0, 10) + " 12:00:00",
                        "volunteer_id", uitgevoerdDoor,
                        "status", "CUSTOMER_INFORMED"
                    ));

                    mariadbJdbi.insert("ticket_log", Map.of(
                        "ticket_id", row.get("reparatienummer"),
                        "date", String.valueOf(row.get("datum gebeld2")).substring(0, 10) + " 12:00:00",
                        "volunteer_id", uitgevoerdDoor,
                        "log", row.get("reactie gebeld2")
                    ));
                }
            }

            if (row.containsKey("datum gebeld1") && row.get("datum gebeld1") != null) {
                if (!ready) {

                    mariadbJdbi.insert("ticket_status", Map.of(
                        "ticket_id", row.get("reparatienummer"),
                        "date", String.valueOf(row.get("datum gebeld1")).substring(0, 10) + " 07:00:00",
                        "volunteer_id", uitgevoerdDoor,
                        "status", "CUSTOMER_INFORMED"
                    ));

                    mariadbJdbi.insert("ticket_log", Map.of(
                        "ticket_id", row.get("reparatienummer"),
                        "date", String.valueOf(row.get("datum gebeld1")).substring(0, 10) + " 07:00:00",
                        "volunteer_id", uitgevoerdDoor,
                        "log", row.get("reactie gebeld1")
                    ));
                }
            }

            // --[ CLOSE ]----------------------------------------------------------------------------------------------
            final List<Map<String, Object>> invoices = accessJdbi.select("SELECT DISTINCT tbl_factuur.datum FROM tbl_factuur RIGHT JOIN (tbl_reparaties_main LEFT JOIN tbl_factuur_omschrijvingen ON tbl_reparaties_main.computernummer = tbl_factuur_omschrijvingen.computernummer) ON tbl_factuur.factuurnummer = tbl_factuur_omschrijvingen.factuurnummer WHERE tbl_factuur.factuurnummer IS NOT NULL AND tbl_reparaties_main.reparatienummer=%s ORDER BY tbl_factuur.datum DESC;".formatted(row.get("reparatienummer")));
            if (!invoices.isEmpty()) {
                val invoice = invoices.getFirst();
                if (invoice != null && invoice.containsKey("datum")) {
                    mariadbJdbi.insert("ticket_status", Map.of(
                        "ticket_id", row.get("reparatienummer"),
                        "date", String.valueOf(invoice.get("datum")).substring(0, 10) + " 23:59:59",
                        "volunteer_id", uitgevoerdDoor,
                        "status", "CLOSED"
                    ));
                }
            }

            // --[ LOG ]------------------------------------------------------------------------------------------------
            if (uitgevoerdDoor == 1) {
                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "volunteer_id", uitgevoerdDoor,
                    "log", "Migratie heeft medewerker op 1 gezet, mederwerker [%s] heeft de reparatie aangenomen, echter is deze persoon onbekend.".formatted(String.valueOf(row.get("medewerker")))
                ));
            }
        }
    }

    private void importLogs(final Integer ticketId, final String importDateTimeFrom) throws Exception {
        if (ticketId != null) {
            final List<Map<String, Object>> logs = accessJdbi.select("SELECT * FROM tbl_reparaties_uitgediept WHERE reparatienummer=%s;".formatted(ticketId));
            if (!logs.isEmpty()) {
                int second = 0;
                for (val logRow : logs) {
                    if (logRow != null && logRow.containsKey("rapport") && logRow.get("rapport") != null) {
                        Integer uitgevoerdDoor = LOOKUP_NAME.get(logRow.get("wie"));
                        if (uitgevoerdDoor == null) {
                            uitgevoerdDoor = 1;
                        } else {
                            individualsImporter.importFromAccess(uitgevoerdDoor, importDateTimeFrom);
                        }

                        mariadbJdbi.insert("ticket_log", Map.of(
                            "ticket_id", ticketId,
                            "date", String.valueOf(logRow.get("datum")).substring(0, 10) + " 00:00:0" + second,
                            "volunteer_id", uitgevoerdDoor,
                            "log", logRow.get("rapport")
                        ));
                        second++;
                    }
                }
            }
        }
    }
}
