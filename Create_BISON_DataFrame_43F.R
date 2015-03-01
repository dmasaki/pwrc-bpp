# Create BISON Dataframe and Populate

a<-""
b<-""
c<-""
bsn43f = data.frame(a,b,c) 

## add columns

## create data columns for BISON ingest

row.names(bsn43f)<-NULL #drop row names

bsn43f$clean_provided_scientific_name<-"" # field 1

bsn43f$itis_common_name<-"" # field 2

bsn43f$itis_tsn<-"" # field  3

bsn43f$basis_of_record<-"" # field 4

bsn43f$occurrence_date<-"" # field 5

bsn43f$year<-"" # field 6

bsn43f$provider<-"" # field 7

bsn43f$provider_url<-"" # field 8

bsn43f$resource<-"" # field 9 

bsn43f$resource_url<-"" # field 10

bsn43f$occurrence_url<-"" # field 11

bsn43f$catalog_number<-"" # field 12

bsn43f$collector<-"" # field 13

bsn43f$collector_number<-"" # field 14

bsn43f$valid_accepted_scientific_name<-"" # field 15

bsn43f$valid_accepted_tsn<-"" # field 16

bsn43f$provided_scientific_name<-"" # field 17

bsn43f$provided_tsn<-"" # field 18

bsn43f$latitude<-"" # field 19

bsn43f$longitude<-"" # field 20

bsn43f$verbatim_elevation<-"" # field 21

bsn43f$verbatim_depth<-"" # field 22

bsn43f$calculated_county_name<-"" # field 23

bsn43f$calculated_fips<-"" # field 24

bsn43f$calculated_state_name<-"" # field 25

bsn43f$centroid<-"" # field 26

bsn43f$provided_county_name<-"" # field 27

bsn43f$provided_fips<-"" # field 28

bsn43f$provided_state_name<-"" # field 29

bsn43f$thumb_url<-"" # field 30

bsn43f$associated_media<-"" # field 31

bsn43f$associated_references<-"" # field 32

bsn43f$general_comments<-"" # field 33

bsn43f$id<-"" # field 34 populate id with row number

bsn43f$provider_id<-"" # field 35

bsn43f$resource_id<-"" # field 36

bsn43f$provided_common_name<-"" # field 37

bsn43f$provided_kingdom<-"" # field 38

bsn43f$geodetic_datum<-"" # field 39

bsn43f$coordinate_precision<-"" # field 40

bsn43f$coordinate_uncertainty<-"" # field 41

bsn43f$verbatim_locality<-"" # field 42

bsn43f$iso_country_code<-"" # field 43

# drop columns a,b,c

bsn43f$a<-NULL
bsn43f$b<-NULL
bsn43f$c<-NULL

# remove blank row

# bsn43f<-bsn43f[-1,]
