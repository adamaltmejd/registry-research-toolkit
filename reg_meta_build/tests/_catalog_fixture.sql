-- Explicit synthetic catalog for reader and structural-validator tests.
-- Stable small IDs make injected corruption and query expectations readable.
-- Source preparation and resolution are tested separately through the pipeline.
-- The graph covers shared provider keys, disjoint periods, aliases, lineage,
-- sensitivity flags, blank codes, and external source labels.

DROP TABLE variable_instance;
DROP TABLE variable_alias_build;
DROP TABLE classification_candidate;
DROP TABLE unika_summary;

INSERT INTO register (
    register_id, provider_id, name, purpose, slug
) VALUES
    (1, 1, 'TESTREG', 'Testning', 'testreg'),
    (2, 1, 'OTHERREG', 'Annat syfte', 'otherreg');

INSERT INTO register_variant (
    register_variant_id, register_id, name, description, slug, display_group, panel_entity_key, panel_time_key, panel_time_grain
) VALUES
    (10, 1, 'Individer', 'Alla individer', 'individer', NULL, NULL, NULL, NULL),
    (20, 2, 'Företag', 'Alla företag', 'foretag', NULL, NULL, NULL, NULL);

INSERT INTO register_version (
    regver_id, register_variant_id, registerversionnamn, registerversionbeskrivning, registerversionmatinformation, registerversion_docstaus, registerversion_forstagodkannandedatum, registerversion_senastgodkanddatum
) VALUES
    (100, 10, '2020', 'Version 2020', '', 'Godkänd', '2020-01-01', '2020-12-31'),
    (101, 10, '2021', 'Version 2021', '', 'Godkänd', '2021-01-01', '2021-12-31'),
    (102, 10, '2022', 'Version 2022', '', 'Godkänd', '2022-01-01', '2022-12-31'),
    (200, 20, '2021', 'Version 2021', '', 'Godkänd', '2021-01-01', '2021-12-31');

INSERT INTO population (
    regver_id, name, definition, comment, date_range
) VALUES
    (100, 'Hela befolkningen', 'Alla personer', '', '2020-12-31'),
    (101, 'Hela befolkningen', 'Alla personer', '', '2021-12-31'),
    (102, 'Hela befolkningen', 'Alla personer', '', '2022-12-31'),
    (200, 'Alla företag', 'Samtliga företag', '', '2021-12-31');

INSERT INTO object_type (
    regver_id, name, definition
) VALUES
    (100, 'Person', 'Fysisk person'),
    (101, 'Person', 'Fysisk person'),
    (102, 'Person', 'Fysisk person'),
    (200, 'Företag', 'Juridisk person');

INSERT INTO variable (
    variable_id, register_id, provider_key, slug, name, definition, description, operational_definition, source_register_text, measurement_unit, source_register_id, source_label, deprecated, is_sensitive, is_identifier
) VALUES
    (1, 1, '44', 'kon', 'Kön', 'Personens kön', 'Kön enligt folkbokföring', NULL, '', '', NULL, NULL, 0, 0, 0),
    (2, 1, '100', 'testcol', 'TestVar', 'En testvariabel', 'Beskrivning av test', NULL, '', '', NULL, NULL, 0, 1, 0),
    (3, 1, '200', 'aaocol', 'ÅÄÖVar', 'Variabel med svenska tecken', 'Åäö i beskrivning', NULL, '', '', NULL, NULL, 0, 1, 0),
    (4, 2, '44', 'kon', 'Kön', 'Ägarkön', 'Kön på ägare', NULL, 'TESTREG', '', 1, 'TESTREG', 0, 0, 0),
    (5, 2, '300', 'uniqcol', 'UniqueVar', 'Unik variabel', 'Bara i reg 2', NULL, '', '', NULL, NULL, 0, 0, 1),
    (6, 2, '301', 'parencol', 'ParenVar', 'Variabel med parentes-källa', 'Test av parentesupplösning', NULL, 'Testregistret (TESTREG) : Folkbokföringsuppgifter', '', 1, 'TESTREG', 0, 0, 0),
    (7, 2, '302', 'extcol', 'ExternVar', 'Variabel från externt system', 'Från myndighet utanför MetaPlus', NULL, 'Försäkringskassan', '', NULL, 'Försäkringskassan', 0, 0, 0),
    (8, 2, '303', 'lopnr', 'LopNr', 'Löpnummer', 'Objektets identifierare', NULL, '', '', NULL, NULL, 0, 0, 1);

INSERT INTO value_code (
    code_id, code, label, mapping_count
) VALUES
    (0, '1', 'Man', 2),
    (1, '2', 'Kvinna', 2),
    (2, '', 'Uppgift okänd', 1),
    (3, '2', 'Övriga civilstånd', 1);

INSERT INTO value_set (
    value_set_id, member_hash
) VALUES
    (1, X'1DC5541B5AF92D3811C454567260E2614AF9D04CACD23373CFA3286E802F8A10'),
    (2, X'59846AAED127858B13738931805E021BA22DD27BA98C4174A73FC66C3D3320CB'),
    (3, X'3E046317B5C3967EC97FA57EDA3F7EB97DF5DAE8607CB3C34DCDDB3B21DE7A5F');

INSERT INTO value_set_member (
    value_set_id, code_id
) VALUES
    (1, 0),
    (1, 1),
    (3, 2),
    (2, 3);

INSERT INTO variable_state (
    state_id, variable_id, register_variant_id, valid_from, valid_to, data_type, data_length, delivery_column_name, source_register_text, operational_definition, provenance, value_set_id, value_set_version_label, classification_id
) VALUES
    (1, 1, 10, '2020-01-01', '2021-12-31', 'int', '1', 'Kon', NULL, NULL, NULL, 1, 'Kön', NULL),
    (2, 1, 10, '2022-01-01', '2022-12-31', 'int', '1', 'Kon', NULL, NULL, NULL, NULL, '', NULL),
    (3, 2, 10, '2020-01-01', '2020-12-31', 'varchar', '10', 'TestCol', NULL, NULL, NULL, NULL, '', NULL),
    (4, 3, 10, '2022-01-01', '2022-12-31', 'varchar', '5', 'AaoCol', NULL, NULL, NULL, NULL, '', NULL),
    (5, 4, 20, '2021-01-01', '2021-12-31', 'int', '1', 'KON', 'TESTREG', NULL, NULL, 1, 'Kön', NULL),
    (6, 5, 20, '2021-01-01', '2021-12-31', 'varchar', '20', 'UniqCol', NULL, NULL, NULL, 2, '2', NULL),
    (7, 6, 20, '2021-01-01', '2021-12-31', 'varchar', '10', 'ParenCol', 'Testregistret (TESTREG) : Folkbokföringsuppgifter', NULL, NULL, 3, 'SSYK 2012', NULL),
    (8, 7, 20, '2021-01-01', '2021-12-31', 'varchar', '10', 'ExtCol', 'Försäkringskassan', NULL, NULL, NULL, '', NULL),
    (9, 8, 20, '2021-01-01', '2021-12-31', 'char', '12', 'LopNr', NULL, NULL, NULL, NULL, '', NULL);

INSERT INTO variable_alias (
    variable_id, register_variant_id, delivery_column_name
) VALUES
    (1, 10, 'Kon'),
    (2, 10, 'TestCol'),
    (2, 10, 'TestKolumn'),
    (3, 10, 'AaoCol'),
    (4, 20, 'KON'),
    (5, 20, 'UniqCol'),
    (6, 20, 'ParenCol'),
    (7, 20, 'ExtCol'),
    (8, 20, 'LopNr');

INSERT INTO variable_alias_window (
    variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, provenance
) VALUES
    (2, 10, 'TestCol', '2020-01-01', '2020-12-31', NULL),
    (2, 10, 'TestKolumn', '2020-01-01', '2020-12-31', NULL);

INSERT INTO code_variable_map (
    code_id, variable_id
) VALUES
    (0, 1),
    (1, 1),
    (0, 4),
    (1, 4),
    (3, 5),
    (2, 6);

INSERT INTO variable_state_lineage (
    consumer_state_id, source_state_id, valid_from, valid_to
) VALUES
    (5, 1, '2021-01-01', '2021-12-31');

INSERT INTO variable_state_lineage_warning (
    consumer_state_id, warning_kind, message
) VALUES
    (7, 'no_source_state', 'No source state in register ''testreg'' for variable slug(s) [''parencol''] (any variant).');

INSERT INTO identifier_semantics (
    var_id, variabelnamn, variabeldefinition
) VALUES
    (303, 'LopNr', 'Objektets identifierare');

INSERT INTO timeseries_event (
    timeseries_event_id, namn, handelse, beskrivning, entitet, id1, id2, fil_id
) VALUES
    (1, 'TESTREG', 'Kodändring', 'Kod 3 ändrad', 'Variabel', '100', '', '1');
