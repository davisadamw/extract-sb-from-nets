library(tidyverse)
library(vroom)

subset_function <- function(x, pos) {
  d_init <- x %>% 
    select(DunsNumber:ZipCode, CBSA, 
           Latitude, Longitude,
           LevelCode, EstCat, 
           ends_with('08'),
           ends_with('09'),
           ends_with('10'),
           ends_with('11'),
           ends_with('12'),
           ends_with('13'),
           -starts_with('EmpC'),
           -starts_with('PayDex'),
           -starts_with('DnBRating'),
           -starts_with('HQDuns'),
           -starts_with('SalesC')) 
  
  # identify NAs in employment data (corresponding to businesses not active in these years)
  d_isna <- d_init %>% 
    select(starts_with('Emp')) %>% 
    mutate_all(~ !is.na(.))
  
  # only keep rows where there is at least one non-na value (business active in at least one year 08-13)
  d_vector <- rowSums(d_isna) >= 1
  
  # finally, use this vector to subset your data
  d_init %>% 
    filter(d_vector)
}

# vroom loads data faster by not actually reading the columns until you ask for them ... 
# but it was running into memory issues on my windows computer for some reason, so we'll use read_tsv_chunked instead
# especially useful in this case since we only need a few columns and only rows corresponding to Santa Barbara estabs
nets_ca <- read_tsv_chunked('Raw_Data/NETS2013_CA(with Address).txt',
                            DataFrameCallback$new(subset_function))

# load the naics data using vroom
naics_ca <- vroom('Raw_Data/NAICS2013_CA.txt',
                  col_select = list(DunsNumber,
                                    ends_with('08'),
                                    ends_with('09'),
                                    ends_with('10'),
                                    ends_with('11'),
                                    ends_with('12'),
                                    ends_with('13'),))
naics_ca_data_indices <- naics_ca %>% 
  select(-DunsNumber) %>% 
  mutate_all(~ !is.na(.)) %>% 
  rowSums()

# the duns numbers are in the same order, so we can just stick this onto the side of the other data
nets_ca_with_naics <- bind_cols(nets_ca,
                                naics_ca %>% 
                                  select(-DunsNumber) %>% 
                                  filter(naics_ca_data_indices >= 1))

# now, grab remove businesses that were never located in SB county
nets_sb_indices <- nets_ca %>% 
  select(starts_with('FIPS')) %>% 
  mutate_all( ~ . == '06083') %>% 
  rowSums()

# use this to subset the data
nets_sb_wide <- nets_ca_with_naics %>% 
  filter(nets_sb_indices >= 1)

# i think it'd probably make sense to convert this to long format now, since there are a lot of year-year columns
nets_sb_long <- nets_sb_wide %>% 
  # pivot will work on all columns that end in two numbers
  pivot_longer(matches('[0-9]{2}$'),
               names_to = c('.value', 'year'),
               names_pattern = '([A-Za-z]+)([0-9]{2})') %>% 
  # and again remove non-sb years
  filter(FIPS == '06083') %>% 
  mutate(year = 2000L + as.integer(year))

# finally, identify any moves, to adjust locations as needed ...
# only use origins for this, only keep moves within SB county
# moves are dated by the last year in the origin location
nets_moves <- vroom('Raw_Data/Moves2013_CA(with Address).txt', 
                    col_select = list(DunsNumber, MoveYear, 
                                      OriginAddress, OriginCity, OriginState,
                                      OriginZIP, OriginLatitude, OriginLongitude, 
                                      OriginLevelCode, OriginFIPSCounty)) %>% 
  filter(OriginFIPSCounty == '06083',
         MoveYear >= 2008)

# update locations with moves

# first, grab business-years that occur on or before a move year for that business
nets_sb_movers <- nets_sb_long %>% 
  inner_join(nets_moves, by = 'DunsNumber') %>% 
  filter(year <= MoveYear)

# everything else goes in the never-moved category
nets_sb_nevermove <- nets_sb_long %>% 
  anti_join(nets_sb_movers, by = c('DunsNumber', 'year'))

# businesses that moved multiple times will get one row per year per join
# for multiple-movers, only keep the move that occurs soonest after
# ... then update location to match origin
nets_sb_movers_relevant_year <- nets_sb_movers %>% 
  group_by(DunsNumber, year) %>% 
  filter((MoveYear - year) == min(MoveYear - year)) %>% 
  ungroup() %>% 
  # remove address and XY data from later, replace with move origins
  select(-Address:-ZipCode, -Latitude:-LevelCode) %>% 
  rename_at(vars(starts_with('Origin')), ~str_replace(., 'Origin', '')) %>% 
  select(-MoveYear, -FIPSCounty)
  
# combine the two datasets, rearrange columns
nets_sb_movers_updatedLL <- bind_rows(nets_sb_nevermove,
                                      nets_sb_movers_relevant_year) %>% 
  select(DunsNumber:year, Emp, Sales, SIC, NAICS, -CBSA)

# write results to disk
nets_sb_movers_updatedLL %>% 
  write_tsv('Raw_Data/sb_locations_08x13.tsv')
