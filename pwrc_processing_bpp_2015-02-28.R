# get working director

getwd()

# Import datasets

bpp <- read.csv("~/R/pwrc-bpp/bpp.csv")

specieslu <- read.delim("~/R/pwrc-bpp/bpp_species_lu.txt")

# filter by first seen

bpp_first<-bpp[bpp$first_arrived_dt!='',]

row.names(bpp_first)<-NULL

# filter by last seen

bpp_last<-bpp[bpp$last_seen_dt!='',]

row.names(bpp_last)<-NULL

# filter by next seen

bpp_next<-bpp[bpp$next_seen_dt!='',]

row.names(bpp_next)<-NULL

# drop and rename columns

colnames(bpp_first)

bpp_first<-bpp_first[,-c(9,10,11,14,15)]

colnames(bpp_first)[8]<-"occurrence_date"

colnames(bpp_next)

bpp_next<-bpp_next[,-c(8,10,11,14,15)]

colnames(bpp_next)[8]<-"occurrence_date"

colnames(bpp_last)

bpp_last<-bpp_last[,-c(8,9,10,14,15)]

colnames(bpp_last)[8]<-"occurrence_date"

# bind dataframes

bpp<- rbind(bpp_first,bpp_next,bpp_last)

# join state name

stateabbr <- read.csv("~/R/pwrc-bpp/US_State_Abbrev_StateName.csv")


# Add sqldf library
   
install.packages("sqldf")

library("sqldf", lib.loc="/Library/Frameworks/R.framework/Versions/3.1/Resources/library")

# Add state names to bpp tables

statejoinstr <- "select bpp.*, stateabbr.name_long from bpp left join stateabbr on bpp.state_alpha_cd = stateabbr.name_short"

bpp_join <- sqldf(statejoinstr)


# Add species scientific names

speciesjoinstr <- "select bpp_join.*, specieslu.scientific_name from bpp_join left join specieslu on bpp_join.species_id = specieslu.species_id"

bpp_join <- sqldf(speciesjoinstr)

# merge descriptive info into general comments field

bpp_join$observation_id<-paste('observation-id', bpp_join$observation_id, sep=':')

bpp_join$species_id<-paste('species-id', bpp_join$species_id, sep=':')

bpp_join$town<-paste('town', bpp_join$town, sep=':')

bpp_join$general_comments <- paste(bpp_join$observation_id,bpp_join$species_id,bpp_join$town, sep="|")


# remove temp dataframes

rm(bpp,bpp_first,bpp_last,bpp_next)


# Clean up headers and drop columns

# change column header name to match BISON schema

colnames(bpp_join)

colnames(bpp_join)[2]<-"provided_common_name"

colnames(bpp_join)[4]<-"year"

colnames(bpp_join)[5]<-"iso_country_code"

colnames(bpp_join)[11]<-"provided_state_name"

colnames(bpp_join)[12]<-"provided_scientific_name"


# view column names

colnames(bpp_join)


# drop columns not used in final BISON data load

bpp_join$observation_id<-NULL

bpp_join$species_id<-NULL

bpp_join$state_alpha_cd<-NULL

bpp_join$town<-NULL

colnames(bpp_join)

# write out to file

write.table(bpp_join, file = "bpp_join_2015-02-28.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)


# load text file to complete processing

bpp_join <- read.delim("~/R/pwrc-bpp/bbl_join_2015-02-28.txt")

## create data columns for BISON ingest by running external R script

source("Create_BISON_DataFrame_43F.R") 

bpp_join_temp<-bpp_join

bpp_join_temp$clean_provided_scientific_name<- bpp_join_temp$provided_scientific_name # field 1

bpp_join_temp$itis_common_name<-"" # field 2

bpp_join_temp$itis_tsn<-"" # field  3

bpp_join_temp$basis_of_record<-"observation" # field 4

# bpp_join_temp$occurrence_date<-"" # field 5

# bpp_join_temp$year<-"" # field 6

bpp_join_temp$provider<-"BISON" # field 7

bpp_join_temp$provider_url<-"http://bison.usgs.ornl.gov" # field 8

bpp_join_temp$resource<-"USGS PWRC - Bird Phenology Program" # field 9 

bpp_join_temp$resource_url<-"http://bison.ornl.gov/ipt/resource.do?r=usgs_pwrc_bird_phenology_program_us_birds" # field 10

bpp_join_temp$occurrence_url<-"" # field 11

bpp_join_temp$catalog_number<-"" # field 12

bpp_join_temp$collector<-"" # field 13

bpp_join_temp$collector_number<-"" # field 14

bpp_join_temp$valid_accepted_scientific_name<-"" # field 15

bpp_join_temp$valid_accepted_tsn<-"" # field 16

# bpp_join_temp$provided_scientific_name<-bpp_join_temp$clean_provided_scientific_name # field 17

bpp_join_temp$provided_tsn<-"" # field 18

# bpp_join_temp$latitude<-"" # field 19

# bpp_join_temp$longitude<-"" # field 20

bpp_join_temp$verbatim_elevation<-"" # field 21

bpp_join_temp$verbatim_depth<-"" # field 22

bpp_join_temp$calculated_county_name<-"" # field 23

bpp_join_temp$calculated_fips<-"" # field 24

bpp_join_temp$calculated_state_name<-"" # field 25

bpp_join_temp$centroid<-"" # field 26

bpp_join_temp$provided_county_name<-"" # field 27

bpp_join_temp$provided_fips<-"" # field 28

# bpp_join_temp$provided_state_name<-"" # field 29

bpp_join_temp$thumb_url<-"" # field 30

bpp_join_temp$associated_media<-"" # field 31

bpp_join_temp$associated_references<-"" # field 32

# bpp_join_temp$general_comments<-"" # field 33

bpp_join_temp$id<-"" # field 34 

bpp_join_temp$provider_id<-"" # field 35

bpp_join_temp$resource_id<-"" # field 36

# bpp_join_temp$provided_common_name<-"" # field 37

bpp_join_temp$provided_kingdom<-"Animalia" # field 38

bpp_join_temp$geodetic_datum<-"" # field 39

bpp_join_temp$coordinate_precision<-"" # field 40

bpp_join_temp$coordinate_uncertainty<-"" # field 41

bpp_join_temp$verbatim_locality<-"" # field 42

# bpp_join_temp$iso_country_code<-"" # field 43

# use rbind to order data columns - bsn43f is master order

bpp_join_temp <- rbind(bsn43f,bpp_join_temp)

bpp_join_temp <- bpp_join_temp[-1,] # remove first row

row.names(bpp_join_temp)<-NULL # remove row.names column

# review scientific names and clean

uspecies_bpp<-unique(bpp_join_temp$clean_provided_scientific_name)

# trim clean species names

bpp_join_temp$clean_provided_scientific_name<-gsub(' \\(sp\\) ',' ',bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<- gsub('\\(sp\\) ','',bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub('\\(sp\\)','',bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(' \\(sp\\)','',bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" leuc. x atricap.","",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" b. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" p. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" h. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" a. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" s. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" c. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" l. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" j. "," ",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" x platy.","",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" x chrysopt.","",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" flamm.","",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" a.auratus x cafer"," auratus",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" flamm./hornemanni"," hornemanni",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" alnorum/traillii"," alnorum",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" difficilis/occid."," difficilis",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub(" leuc. x atricap.","",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub("Carduelis/hornemanni","Carduelis hornemanni",bpp_join_temp$clean_provided_scientific_name)

bpp_join_temp$clean_provided_scientific_name<-gsub("Ammodramus nelsoni/caudacut.","Ammodramus nelsoni",bpp_join_temp$clean_provided_scientific_name)

# bpp_join_temp$clean_provided_scientific_name<-gsub(" ()","",bpp_join_temp$clean_provided_scientific_name)

uspecies_bpp<-unique(bpp_join_temp$clean_provided_scientific_name)

# remove records with no species name

bpp_join_temp_blank_sciname<-bpp_join_temp[bpp_join_temp$clean_provided_scientific_name=='',]

bpp_join_temp<-bpp_join_temp[bpp_join_temp$clean_provided_scientific_name!='',]

# subset for review

uspecies_bpp<-unique(bpp_join_temp$clean_provided_scientific_name)
write.table(uspecies_bpp, file = "bison_bpp_uspecies_2015-02-28.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(uspecies_bpp)

bpp_top50<-head(bpp_join_temp)
write.table(bpp_top50, file = "bison_bpp_top50_2015-02-28.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bpp_top50)

bpp_bottom50<-tail(bpp_join_temp)
write.table(bpp_bottom50, file = "bison_bpp_bottom_50_2015-02-28.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bpp_bottom50)

bpp_subset100000<-bpp_join_temp[1:100000,]
write.table(bpp_subset100000, file = "bison_bpp_100k_2015-02-28.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bpp_subset100000)

# write out final bbl data

write.table(bsnbpp, file = "bison_bpp_ordered_final_2015-03-19.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)



