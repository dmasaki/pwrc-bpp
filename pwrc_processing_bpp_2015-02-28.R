# get working director

getwd()

# Import datasets

bpp <- read.csv("~/R/pwrc-bpp/bpp.csv")

specieslu <- read.delim("~/R/pwrc-bpp/bpp_species_lu.txt")

stctylu <- read.csv("~/R/usgs-bbl-2015-03//cntry_state_cnty_lookup.csv")

# Review data

View(bblspecies)

View(specieslu)

View(stctylu)

# drop columns

bblspecies$X<-NULL

specieslu$X...<-NULL

stctylu$X...<-NULL

# Add sqldf library
   
install.packages("sqldf")

library("sqldf", lib.loc="/Library/Frameworks/R.framework/Versions/3.1/Resources/library")

# Add species names to bblspecies dataframe

# SPECIES_ID in bblspecies is our key for retrieving scientific/common name

speciesjoinstr <- "select bblspecies.*, specieslu.SCI_NAME, specieslu.SPECIES_NAME, specieslu.ALPHA_CODE from bblspecies left join specieslu on bblspecies.SPECIES_ID = specieslu.SPECIES_ID"

bblspecies_join <- sqldf(speciesjoinstr)

# change date format ex. "12/14/98" >> "1998-12-14"

bblspecies_join$iso_date<-as.Date(strptime(bblspecies_join$Banding.Date,'%Y-%m-%d'))

# extract year from date

bblspecies_join$year<-format(bblspecies_join$iso_date, "%Y") 

# Add geographic information from state county lookup

# Clean up code fields

# County code should be 3 character string

stctylu$COUNTY_CODE<-sprintf("%03d",stctylu$COUNTY_CODE)

bblspecies_join$COUNTY_CODE<-sprintf("%03d",bblspecies_join$COUNTY_CODE)

# State code should be 2 character string

stctylu$STATE_CODE<-sprintf("%02d",stctylu$STATE_CODE)

bblspecies_join$STATE_CODE<-sprintf("%02d",bblspecies_join$STATE_CODE)

# Concatenate state and county code to get 5 character FIPS

bblspecies_join$FIPS <- paste(bblspecies_join$STATE_CODE, bblspecies_join$COUNTY_CODE, sep='')

bblspecies_join$CFIPS <- paste(bblspecies_join$COUNTRY_CODE, bblspecies_join$FIPS, sep='')

stctylu$FIPS <- paste(stctylu$STATE_CODE, stctylu$COUNTY_CODE, sep='')

stctylu$CFIPS <- paste(stctylu$COUNTRY_CODE, stctylu$FIPS, sep='')

# Join bblspecies_join and state lookup to retrieve state county

stcntyjoinstr <- "select bblspecies_join.*, stctylu.STATE_NAME, stctylu.COUNTY_NAME, stctylu.COUNTY_DESCRIPTION from bblspecies_join left join stctylu on bblspecies_join.CFIPS = stctylu.CFIPS"

bblspecies_join_temp <- sqldf(stcntyjoinstr)

# if join returns same number of rows as original then commit final

bblspecies_join <- bblspecies_join_temp

# remove temp dataframes

rm(bblspecies_join_temp)

rm(bblspecies)

# Clean up headers and drop columns

# change column header name to match BISON schema

colnames(bblspecies_join)

colnames(bblspecies_join)[2]<-"latitude"

colnames(bblspecies_join)[3]<-"longitude"

colnames(bblspecies_join)[5]<-"iso_country_code"

colnames(bblspecies_join)[8]<-"provided_fips"

colnames(bblspecies_join)[9]<-"clean_provided_scientific_name"

colnames(bblspecies_join)[10]<-"provided_common_name"

colnames(bblspecies_join)[13]<-"occurrence_date"

colnames(bblspecies_join)[15]<-"provided_state_name"

colnames(bblspecies_join)[16]<-"provided_county_name"

# view column names

colnames(bblspecies_join)


# drop columns not used in final BISON data load

bblspecies_join$SPECIES_ID<-NULL

bblspecies_join$Banding.Date<-NULL

bblspecies_join$STATE_CODE<-NULL

bblspecies_join$COUNTY_CODE<-NULL

bblspecies_join$ALPHA_CODE<-NULL

bblspecies_join$CFIPS<-NULL

bblspecies_join$COUNTY_DESCRIPTION<-NULL

# write out to file

write.table(bblspecies_join, file = "bbl_join_2015-02-22.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)

# clear objects in memory restart R studio 

# load text file to complete processing

bblspecies_join <- read.delim("~/R/bbl_join_2015-02-22.txt")

# add BISON columns and order using template and datamerge library

install.packages("datamerge")

library("datamerge", lib.loc="/Library/Frameworks/R.framework/Versions/3.1/Resources/library")

## create data columns for BISON ingest by running external R script

source("Create_BISON_DataFrame_43F.R") 

# create subset to work against

bblspecies_subset<-bblspecies_join

# use template to order data columns 

bblspecies_join<-version.merge(bsn43f, bblspecies_join, add.rows=TRUE, add.cols=TRUE, add.values=TRUE, verbose=TRUE)

# write out final bbl data

write.table(bblspecies_join, file = "bison_bbl_us10min_final_2015-02-22.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)




