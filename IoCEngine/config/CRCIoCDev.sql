create table sb2file_ctgry_codes
(
	id integer,
	code_name varchar(8),
	header_code varchar(8),
	file_type_code varchar(8)
);

alter table sb2file_ctgry_codes owner to postgres;

create table data_providers
(
	code_name varchar(16),
	dpid varchar(10),
	sbmxn_pt varchar(5),
	ctgry varchar(5),
	typ varchar(5),
	day_first boolean,
	id serial not null
);

alter table data_providers owner to postgres;

create table aa_udf_catalogues
(
	owner text,
	segment text,
	udf_cat_code text,
	udf_cat_value text,
	crc_cat_code text,
	crc_cat_value text,
	catalogue_name text,
	field_code text,
	note text,
	id serial not null
);

alter table aa_udf_catalogues owner to postgres;

