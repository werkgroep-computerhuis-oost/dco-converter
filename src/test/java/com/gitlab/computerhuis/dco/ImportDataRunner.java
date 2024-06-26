package com.gitlab.computerhuis.dco;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gitlab.computerhuis.dco.importer.*;
import com.gitlab.computerhuis.dco.jdbi.AccessJdbi;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
import com.gitlab.computerhuis.dco.util.MapperUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ImportDataRunner {

    private static final String IMPORT_DATE_FROM = "2020-01-01";
    private static final String IMPORT_DATE_TIME_FROM = IMPORT_DATE_FROM + "T00:00:00";
    private static final ObjectMapper mapper = MapperUtils.createJsonMapper();

    private static AccessJdbi accessJdbi;
    private static MariadbJdbi mariadbJdbi;

    @BeforeAll
    public static void beforeAll() throws Exception {
        accessJdbi = AccessJdbi.getInstance();
        mariadbJdbi = MariadbJdbi.getInstance();
    }

    @Test
    @Order(1)
    void import_postal_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        postalCodeImporter.importFromJson();
    }

    @Test
    @Order(2)
    void import_activities_from_json() throws Exception {
        val activitiesImporter = new ActivitiesImporter(mariadbJdbi, mapper);
        activitiesImporter.importFromJson();
    }

    @Test
    @Order(3)
    void import_individuals_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        individualsImporter.importFromJson();
    }

    @Test
    @Order(4)
    void import_individual_login_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        individualsImporter.importLoginsFromJson();
    }

    @Test
    @Order(5)
    void import_individual_authorities_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        individualsImporter.importAuthoritiesFromJson();
    }

    @Test
    @Order(6)
    void import_equipment_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        val equipmentImporter = new EquipmentImporter(accessJdbi, mariadbJdbi, mapper, individualsImporter);
        equipmentImporter.importFromAcces(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(7)
    void import_individuals_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        individualsImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(8)
    void import_timesheets_from_access() throws Exception {
        val timesheetImporter = new TimesheetImporter(accessJdbi, mariadbJdbi);
        timesheetImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(9)
    void import_tickets_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(mariadbJdbi, mapper);
        val individualsImporter = new IndividualsImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        val equipmentImporter = new EquipmentImporter(accessJdbi, mariadbJdbi, mapper, individualsImporter);
        val ticketImporter = new TicketImporter(accessJdbi, mariadbJdbi, mapper, equipmentImporter, individualsImporter);
        ticketImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }
}
