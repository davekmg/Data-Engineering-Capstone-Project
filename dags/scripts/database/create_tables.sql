
CREATE TABLE public.dim_airlines (
	airline_key VARCHAR(10)NOT NULL,
	airline_name VARCHAR(200),
	CONSTRAINT dim_airlines_pkey PRIMARY KEY (airline_key)
);


CREATE TABLE public.dim_countries (
	country_key INT NOT NULL,
	country_code INT,
	country_iso_code VARCHAR(2),
	country_name VARCHAR(100),
	CONSTRAINT dim_countries_pkey PRIMARY KEY (country_key)
);


CREATE TABLE public.dim_date (
	date_key DATE NOT NULL,
	date DATE,
	year INT,
	quarter INT,
	month INT,
	day INT,
	week INT,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);


CREATE TABLE public.dim_port_of_entry (
	port_of_entry_key varchar(3) NOT NULL,
	port_of_entry_name varchar(100),
	CONSTRAINT dim_port_of_entry_pkey PRIMARY KEY (port_of_entry_key)
);

CREATE TABLE public.dim_states (
	state_key VARCHAR(2) NOT NULL,
	state_name VARCHAR(50),
	CONSTRAINT dim_states_pkey PRIMARY KEY (state_key)
);


CREATE TABLE public.dim_travel_modes (
	travel_mode_key INT NOT NULL,
	travel_mode_name VARCHAR(30),
	CONSTRAINT dim_travel_modes_pkey PRIMARY KEY (travel_mode_key)
);


CREATE TABLE public.dim_visa_categories (
	visa_category_key INT NOT NULL,
	visa_category_name VARCHAR(30),
	CONSTRAINT dim_visa_categories_pkey PRIMARY KEY (visa_category_key)
);


CREATE TABLE public.fact_immigration (
	id INT NOT NULL,
	country_citizen_key INT,
	country_resident_key INT,
	port_of_entry_key VARCHAR(3),
	arrival_date_key DATE,
	travel_mode_key INT,
	state_key VARCHAR(2),
	departure_date_key DATE,
	age INT,
	visa_category_key INT,
	match_flag BOOLEAN,
	gender VARCHAR(1),
	ins_num INT,
	airline_key VARCHAR(10),
	admission_number INT,
	flight_number VARCHAR(20),
	visa_type VARCHAR(10),
	CONSTRAINT fact_immigration_pkey PRIMARY KEY (id)
);


CREATE TABLE public.fact_temperature (
	id VARCHAR(36) NOT NULL,
	date_key DATE,
	country_key INT,
	average_temperature DOUBLE PRECISION,
	average_temperature_uncertainty DOUBLE PRECISION,
	CONSTRAINT fact_temperature_pkey PRIMARY KEY (id)
);


CREATE TABLE public.fact_us_population (
	us_population_id VARCHAR(36) NOT NULL,
	state_key VARCHAR(2),
	city VARCHAR(100),
	median_age DOUBLE PRECISION,
	male_population INT,
	female_population INT,
	total_population INT,
	number_of_veterans INT,
	foreign_born INT,
	CONSTRAINT fact_us_population_pkey PRIMARY KEY (us_population_id)
);


CREATE TABLE public.fact_us_race (
	us_race_id VARCHAR(36) NOT NULL,
	state_key VARCHAR(2),
	city VARCHAR(100),
	count INT,
	CONSTRAINT fact_us_race_pkey PRIMARY KEY (us_race_id)
);
