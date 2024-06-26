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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@RequiredArgsConstructor
public class EquipmentImporter {

    private final AccessJdbi accessJdbi;
    private final MariadbJdbi mariadbJdbi;
    private final ObjectMapper mapper;
    private final IndividualsImporter individualsImporter;

    public void importFromAcces(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing equipment from access");
        final List<Map<String, Object>> equipment = accessJdbi.select("SELECT Tbl_computers.* FROM Tbl_computers WHERE [Datum Gift] > #%s#".formatted(importDateFrom));
        for (val item : equipment) {
            importFromAcces(item, importDateTimeFrom, importDateTimeFrom);
        }
    }

    public void importFromAcces(final Integer computerId, final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing individual {} from access", computerId);
        final List<Map<String, Object>> equipment = accessJdbi.select("SELECT Tbl_computers.* FROM Tbl_computers WHERE Computernummer=%s".formatted(computerId));
        for (val item : equipment) {
            importFromAcces(item, importDateTimeFrom, importDateTimeFrom);
        }
    }

    private void importFromAcces(final Map<String, Object> computer, final String importDateFrom, final String importDateTimeFrom) throws Exception {
        if (computer != null && !computer.isEmpty() && !mariadbJdbi.exist("equipment", "id", computer.get("computernummer"))) {

            String category = String.valueOf(computer.get("type kast"));
            switch (category.toUpperCase().trim()) {
                case "LAPTOP":
                case "LAPTUP":
                case "NOTEBOOK":
                    category = "LAPTOP";
                    break;
                case "MOBILE":
                case "MOBIEL":
                case "SMARTPHONE":
                    category = "MOBILE";
                    break;
                case "SIM":
                case "SIMKAART":
                    category = "SIM";
                    break;
                case "TABLET":
                    category = "TABLET";
                    break;
                case "USB STICK":
                    category = "USB_STICK";
                    break;
                case "MINI TOWER":
                case "TOWER":
                case "DESKTOP":
                case "NULL":
                case "GEEN":
                case "":
                    category = "DESKTOP";
                    break;
                default:
                    throw new RuntimeException("Unsupported type: " + category);
            }

            String status = switch ((isNotBlank((String) computer.get("Status"))) ? String.valueOf(computer.get("Status")).trim().toUpperCase() : "") {
                case "BINNENGEKOMEN GIFT" -> "INCOMING_GIFT";
                case "GESCHIKT VOOR VERKOOP" -> "SUITABLE_FOR_GIFT";
                case "KLAAR VOOR VERKOOP" -> "RESERVED";
                case "KLANT PC" -> "CUSTOMER_PC";
                case "VERKOCHT" -> "GIFT_ISSUED";
                case "SLOOP" -> "DEMOLITION";
                default -> null;
            };

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (computer.get("datum gift") != null && computer.get("datum gift") instanceof Timestamp) {
                registered = ((Timestamp) computer.get("datum gift")).toLocalDateTime();
            }

            String fabrikant = (isNotBlank((String) computer.get("fabrikant"))) ? String.valueOf(computer.get("fabrikant")).trim() : "Onbekend";

            Map<String, String> specificatie = new HashMap<>();
            if (isNotBlank((String) computer.get("processor"))) {
                specificatie.put("processor", String.valueOf(computer.get("processor")).trim());
            }
            if (isNotBlank((String) computer.get("geheugen"))) {
                specificatie.put("geheugen", String.valueOf(computer.get("geheugen")).trim());
            }
            if (isNotBlank((String) computer.get("hdd ssd"))) {
                specificatie.put("harddisk", String.valueOf(computer.get("hdd ssd")).trim());
            }
            if (isNotBlank((String) computer.get("optische apparaten"))) {
                specificatie.put("optisch", String.valueOf(computer.get("optische apparaten")).trim());
            }
            if (Boolean.TRUE.equals(computer.get("cardreader"))) {
                specificatie.put("cardreader", String.valueOf(computer.get("cardreader")));
            }
            if (isNotBlank((String) computer.get("videokaart"))) {
                specificatie.put("videokaart", String.valueOf(computer.get("videokaart")).trim());
            }
            if (isNotBlank((String) computer.get("overige ingebouwde apparaten"))) {
                specificatie.put("overige", String.valueOf(computer.get("overige ingebouwde apparaten")).trim());
            }
            if (isNotBlank((String) computer.get("bijzonderheden"))) {
                specificatie.put("bijzonderheden", String.valueOf(computer.get("bijzonderheden")).trim());
            }
            if (isNotBlank((String) computer.get("software"))) {
                specificatie.put("software", String.valueOf(computer.get("software")).trim());
            }

            Map<String, Object> row = new HashMap<>();
            row.put("category", category);
            row.put("id", computer.get("computernummer"));
            row.put("manufacturer", fabrikant);
            row.put("model", (isNotBlank((String) computer.get("model nummer"))) ? ((String) computer.get("model nummer")).trim() : null);
            row.put("specification", mapper.writeValueAsString(specificatie));
            row.put("customer_id", computer.get("gebruikersnummer"));
            row.put("status", status);
            row.put("registered", registered);

            log.info("Insert: {}", row);
            individualsImporter.importFromAccess((Integer) computer.get("gebruikersnummer"), importDateTimeFrom);
            mariadbJdbi.insert("equipment", row);
        }
    }
}
