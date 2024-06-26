package com.gitlab.computerhuis.dco.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gitlab.computerhuis.dco.jdbi.AccessJdbi;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
import com.gitlab.computerhuis.dco.util.PathTestUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.*;

@Slf4j
@RequiredArgsConstructor
public class IndividualsImporter {

    private final AccessJdbi accessJdbi;
    private final MariadbJdbi mariadbJdbi;
    private final PostalCodeImporter postalCodeImporter;
    private final ObjectMapper mapper;

    public void importFromJson() throws IOException {
        log.info("Start importing individuals from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/individuals.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("individuals", "id", row.get("id"))) {
                mariadbJdbi.insert("individuals", row);
            }
        }
    }

    public void importLoginsFromJson() throws IOException {
        log.info("Start importing individual login from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/individual_login.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("individual_login", "username", row.get("username"))) {
                mariadbJdbi.insert("individual_login", row);
            }
        }
    }

    public void importAuthoritiesFromJson() throws IOException {
        log.info("Start importing individual authorities from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/individual_authorities.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("individual_authorities", "username", row.get("username"))) {
                mariadbJdbi.insert("individual_authorities", row);
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCESS
    // -----------------------------------------------------------------------------------------------------------------
    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing individuals from access");
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE [Datum Inschrijving] > #%s#".formatted(importDateFrom));
        for (val row : rows) {
            importFromAccess(row, importDateTimeFrom);
        }
    }

    public void importFromAccess(final Integer gebruikersnummer, final String importDateTimeFrom) throws Exception {
        log.info("Start importing individual {} from access", gebruikersnummer);
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE Gebruikersnummer=%s".formatted(gebruikersnummer));
        for (val row : rows) {
            importFromAccess(row, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> individual, final String importDateTimeFrom) throws Exception {
        if (individual != null && !individual.isEmpty() && !mariadbJdbi.exist("individuals", "id", individual.get("gebruikersnummer"))) {
            Integer huisnummer = cleanup_house_number((String) individual.get("huisnummer"));
            String huisnummertoevoeging = cleanup_huisnummer_toevoeging(String.valueOf(huisnummer), (String) individual.get("huisnummer"));

            String mobile = null;
            String telefoon = null;

            if (isNotBlank((String) individual.get("1e telefoon")) && ((String) individual.get("1e telefoon")).trim().startsWith("06")) {
                mobile = (String) individual.get("1e telefoon");
            } else {
                telefoon = (String) individual.get("1e telefoon");
            }

            if (isNotBlank((String) individual.get("2e telefoon")) && ((String) individual.get("2e telefoon")).trim().startsWith("06")) {
                mobile = (String) individual.get("2e telefoon");
            } else {
                telefoon = (String) individual.get("2e telefoon");
            }
            String woonplaats = "'s-Hertogenbosch";
            if (isNotBlank((String) individual.get("plaatsnaam"))) {
                woonplaats = (String) individual.get("plaatsnaam");
            }

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (individual.get("datum inschrijving") != null && individual.get("datum inschrijving") instanceof Timestamp) {
                registered = ((Timestamp) individual.get("datum inschrijving")).toLocalDateTime();
            }

            String postal_code = null;
            if (isNotBlank((String) individual.get("postalcode"))) {
                postal_code = ((String) individual.get("postcode")).trim().replace(" ", "");
            }

            Map<String, Object> row = new HashMap<>();
            row.put("id", individual.get("gebruikersnummer"));
            row.put("initials", individual.get("voorletters"));
            row.put("first_name", individual.get("voornaam"));
            row.put("infix", individual.get("tussenvoegsels"));
            row.put("last_name", individual.get("achternaam"));
            row.put("date_of_birth", individual.get("geboortedatum"));
            row.put("email", individual.get("e-mailadres"));
            row.put("mobile", mobile);
            row.put("telephone", telefoon);
            row.put("postal_code", postal_code);
            row.put("street", individual.get("adres"));
            row.put("house_number", huisnummer);
            row.put("house_number_addition", huisnummertoevoeging);
            row.put("city", woonplaats);
            row.put("registered", registered);
            row.put("comments", individual.get("opmerkingen"));
            row.put("msaccess", mapper.writeValueAsString(individual));

            if (!postalCodeImporter.doesPostalCodeExist(postal_code, huisnummer)) {
                row.remove("postal_code");
                row.remove("house_number");
                row.remove("house_number_addition");
            }

            mariadbJdbi.insert("individuals", row);

            Integer bedrijfsnummer = (Integer) individual.get("bedrijfsnummer");
            if (bedrijfsnummer != null && (bedrijfsnummer == 6 || bedrijfsnummer == 1)) {
                String user_type = "VOLUNTEER";
                if (bedrijfsnummer == 6) {
                    user_type = "CANDIDATE";
                }

                Map<String, Object> login = new HashMap<>();
                login.put("volunteer_id", individual.get("gebruikersnummer"));
                login.put("username", individual.get("e-mailadres"));
                login.put("user_type", user_type);
                mariadbJdbi.insert("individual_login", login);

                for (var role : List.of("ROLE_COUNTER", "ROLE_EDUCATION", "ROLE_WORKSHOP")) {
                    Map<String, Object> login_role = new HashMap<>();
                    login_role.put("username", individual.get("e-mailadres"));
                    login_role.put("authority", role);
                    mariadbJdbi.insert("individual_authorities", login_role);
                }
            }
        }
    }

    private Integer cleanup_house_number(final String house_number) {
        if (isBlank(house_number)) {
            return null;
        }

        if (isNumeric(house_number)) {
            return Integer.valueOf(house_number);
        }

        return Integer.parseInt(house_number.replaceAll("[^0-9]", ""));
    }

    private String cleanup_huisnummer_toevoeging(final String house_number, final String house_number_addition) {
        if (isBlank(house_number_addition)) {
            return null;
        }

        return house_number_addition.replace(house_number, "");
    }
}
