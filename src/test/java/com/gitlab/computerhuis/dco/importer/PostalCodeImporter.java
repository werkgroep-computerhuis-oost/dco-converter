package com.gitlab.computerhuis.dco.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gitlab.computerhuis.dco.jdbi.MariadbJdbi;
import com.gitlab.computerhuis.dco.util.PathTestUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@RequiredArgsConstructor
public class PostalCodeImporter {

    private final MariadbJdbi mariadbJdbi;
    private final ObjectMapper mapper;

    public void importFromJson() throws Exception {
        log.info("Start importing postal from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/postal_codes.json");
        val data = mapper.readValue(jsonString, Map.class);

        val provincies = (Map<String, Map>) data.get("provincies");
        for (val provincie : provincies.entrySet()) {
            val gemeenten = (Map<String, Map>) provincie.getValue().get("gemeenten");
            for (val gemeente : gemeenten.entrySet()) {
                val plaatsen = (Map<String, Map>) gemeente.getValue().get("plaatsen");
                for (val plaats : plaatsen.entrySet()) {

                    val postbussen = (Map<String, Map<String, String>>) plaats.getValue().get("postbussen");
                    for (val postbus : postbussen.entrySet()) {
                        if (!mariadbJdbi.exist("postal_codes", "code", postbus.getKey())) {
                            val row = new HashMap<String, Object>();
                            row.put("code", postbus.getKey());
                            row.put("province", gemeente.getKey());
                            row.put("municipality", gemeente.getKey());
                            row.put("city", plaats.getKey());
                            row.put("pobox", true);
                            row.put("url", postbus.getValue());
                            mariadbJdbi.insert("postal_codes", row);
                        }
                    }

                    val wijken = (Map<String, Map>) plaats.getValue().get("wijken");
                    for (val wijk : wijken.entrySet()) {
                        val buurten = (Map<String, Map>) wijk.getValue().get("buurten");
                        for (val buurt : buurten.entrySet()) {
                            val postcodes = (Map<String, Map>) buurt.getValue().get("postcodes");
                            for (val postcode : postcodes.entrySet()) {
                                if (!mariadbJdbi.exist("postal_codes", "code", postcode.getKey())) {
                                    val row = new HashMap<String, Object>();
                                    row.put("code", postcode.getKey());
                                    row.put("province", provincie.getKey());
                                    row.put("municipality", gemeente.getKey());
                                    row.put("city", plaats.getKey());
                                    row.put("district", wijk.getKey());
                                    row.put("neighbourhood", buurt.getKey());
                                    row.put("street", postcode.getValue().get("straat"));
                                    row.put("house_number_min", ((String) postcode.getValue().get("nummers")).split("-")[0].strip());
                                    row.put("house_number_max", ((String) postcode.getValue().get("nummers")).split("-")[1].strip());
                                    row.put("url", postcode.getValue().get("url"));
                                    mariadbJdbi.insert("postal_codes", row);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean doesPostalCodeExist(final String postal_code, final Integer house_number) {
        if (isBlank(postal_code) || house_number == null) {
            return false;
        }

        val sql = "SELECT TRUE AS exist FROM postal_codes WHERE code=:code AND house_number_min>=:number AND house_number_max<=:number";
        val found = mariadbJdbi.getHandle().createQuery(sql)
            .bind("code", postal_code)
            .bind("number", house_number)
            .mapTo(Boolean.class).findOne();
        return found.orElse(false);
    }
}
