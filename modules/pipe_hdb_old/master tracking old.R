source('packages/precode-r/pre_runr.R')
library(scales)  # For formatting labels

runpre('pre_wrappers.R')

# data_2017 <- read_csv('data/govdata/Resale flat prices based on registration date from Jan-2017 onwards.csv')

if (!exists("data_2017")) {
  source('modules/pipe_hdb/fetch_hdb_data.R')
  data_2017 <- fetch_hdb_data()
}
glimpse(data_2017)

hdbdata = bind_rows(data_2017)

hdbdata <- hdbdata %>% mutate(
  tx_monthdate = ym(month),
  tx_year = year(tx_monthdate),
)


hdbdata <- hdbdata %>% mutate(covid = 
                                case_when(tx_year >= 2022 ~ 'after 2022',
                                          tx_year <2022 ~ 'before 2022'))

abc = sortcount(hdbdata, 'town')

sortcount(hdbdata, 'flat_type')
sortcount(hdbdata, 'floor_area_sqm')
# sortcount(hdbdata, 'flat_model')
sortcount(hdbdata, 'lease_commence_date')
sortcount(hdbdata, 'tx_year')

hdbdata <- hdbdata %>% 
  mutate(
    age_sold = tx_year - lease_commence_date,
    remaining_lease_sold = 99-age_sold,
    pretend_top_2025 = 2025-age_sold)


hi <- hdbdata %>% group_by(tx_monthdate) %>% count()


######## 

glimpse(hdbdata)



hdb_sel <- hdbdata %>% 
  # filter(town == 'BEDOK') %>%
  # filter(town == 'WOODLANDS') %>%
  # filter(town == 'TAMPINES') %>%
  
  # filter(street_name %in% c('TAMPINES AVE 7',
  #                           'TAMPINES ST 23',
  #                           'TAMPINES ST 24',
  #                           'TAMPINES ST 32',
  #                           'TAMPINES ST 42' )) %>%
  # filter(street_name == 'TAMPINES CTRL 8') %>%
  
  # filter(street_name == 'TAMPINES ST 22') %>%
  # filter(street_name ==  'WOODLANDS DR 16') %>%
  # filter(str_detect(street_name, 'JLN BATU')) %>%
  
  filter(remaining_lease_sold <= 75) %>%
  # filter(remaining_lease_sold <= 85) %>%
  select(tx_year, tx_monthdate, covid, flat_type, resale_price, age_sold, remaining_lease_sold, pretend_top_2025, street_name, storey_range) %>%
  filter(flat_type %in% c('4 ROOM')) %>%
  # filter(flat_type %in% c('3 ROOM')) %>%
  arrange(desc(tx_monthdate))

abc = sortcount(hdb_sel, 'street_name')

#### 
print('asjust prices for inflation')
####

hdb_median <- hdb_sel %>%
  # filter(pretend_top_2025 >= 1998) %>%
  group_by(tx_year, flat_type) %>%
  summarize(
    median_price = median(resale_price), 
    nb_sales = n(), 
    .groups = 'drop') %>%
  arrange(flat_type, tx_year, median_price)

locate_maxyear <- hdb_median %>% group_by(flat_type) %>% 
  filter(tx_year <= 2022) %>%
  filter(tx_year==max(tx_year)) %>%
  rename(median_price_anchor = median_price) %>% select(-tx_year)

hdb_inflation <- hdb_median %>% left_join(locate_maxyear, by='flat_type') %>%
  mutate(inflation = median_price_anchor/median_price)
hdb_inflation <- hdb_inflation %>% select(tx_year, flat_type, inflation)
hdb_inflated <- hdb_sel %>% left_join(hdb_inflation) %>% mutate(resale_price_proxy_2025 = inflation*resale_price)

####################
print('did prices increae over the years')
####################

# Compute median resale price per tx_year per flat_type
hdb_median <- hdb_sel %>%
  filter(tx_monthdate >= '2022-01-01') %>%
  group_by(tx_monthdate, flat_type) %>%
  summarize(
    median_price = median(resale_price), 
    samplesize = n(), 
    .groups = 'drop') %>%
  arrange(desc(tx_monthdate))

# Create count summary (must match facet grouping)
hdb_count <- hdb_sel %>%
  filter(tx_monthdate >= '2022-01-01') %>%
  group_by(tx_monthdate, flat_type) %>%
  summarise(count = n(), .groups = "drop")



scale_min = min(hdb_median$median_price, na.rm = TRUE) %>% round(-4)
scale_max = max(hdb_median$median_price, na.rm = TRUE) %>% round(-4)

scale_factor <-(scale_max - scale_min) / (max(hdb_count$count, na.rm = TRUE))


# Plot
ggplot(hdb_sel, aes(x = tx_monthdate, y = resale_price)) +
  # geom_boxplot(alpha = 0.5, outlier.shape = NA, fill = "lightblue") +  # Box plot per year
  geom_line(data = hdb_median, aes(x = tx_monthdate, y = median_price), color = "steelblue", size = 1.2) +  # Median price line
  geom_text(data = hdb_median, aes(x = tx_monthdate, y = median_price, 
                                   label = label_number(scale = 1e-3, suffix = "K", accuracy = 1)(median_price)), 
            vjust = -0.5, color = "black", size = 3) +  # 
  
  geom_col(data = hdb_count,
           aes(x = tx_monthdate, y = count * scale_factor + scale_min), 
           fill = "black", alpha = 0.4, inherit.aes = FALSE) +
  
  geom_text(data = hdb_count,
            aes(x = tx_monthdate, y = scale_min, label = comma(count)),
            color = "gray30", size = 2.5, vjust = 0, inherit.aes = FALSE) +
  
  coord_cartesian(ylim = c(scale_min, NA)) +
  scale_y_continuous(breaks = seq(0, scale_max, by = 10000)) +
  facet_grid(. ~ flat_type) +  # Facet by flat_type
  # theme_minimal() +
  # scale_y_continuous(limits = c(200000, 700000), breaks = seq(0, 1000000, by = 100000), 
  #                    labels = label_number(scale = 1e-3, suffix = "K", accuracy = 1)) + 
  # scale_x_continuous(breaks = seq(min(hdb_sel$tx_monthdate), max(hdb_sel$tx_monthdate), by = 1)) +  # Display all years on x-axis
  labs(title = paste0("HDB Resale Prices Over Time by Flat Type: "),
       x = "Transaction Year",
       y = "Resale Price") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# 
# # If the prices of four room has kept increasing, then how come the age did not make it drop?
# # Price over TOP
# 
# ###################
# print('price vs TOP')
# ##################
# 
# target = hdb_inflated
# # target = hdb_sel
# 
# top_median <- target %>%
#   filter(flat_type %in% c('2 ROOM', '3 ROOM', '4 ROOM')) %>%
#   filter(age_sold >= 6) %>%
#   group_by(flat_type, pretend_top_2025, remaining_lease_sold, age_sold, covid) %>%
#   summarize(
#     median_price = median(resale_price_proxy_2025), .groups = 'drop',
#     # median_price = median(resale_price), .groups = 'drop',
#     sample_size=n()) %>%
#   arrange(flat_type, pretend_top_2025, remaining_lease_sold) %>%
#   filter(covid == 'after 2022')
# 
# # top_median <- top_median %>% filter(flat_type == '2 ROOM')
# 
# library(ggplot2)
# library(dplyr)
# 
# 
# # i need to correct for 2025 prices
# # Plot
# ggplot(top_median, aes(x = pretend_top_2025, y = median_price)) +  
#   geom_point(aes(color = flat_type, size = sample_size)) +  # Scatter plot of points
#   geom_line(aes(group = 1), color = "blue") +  # Add a line connecting the points
#   facet_grid(covid ~ flat_type) +  # Facet grid by flat_type and covid
#   # theme_minimal() +
#   scale_y_continuous(limits = c(300000, 700000), breaks = seq(0, 1000000, by = 100000), 
#                      labels = label_number(scale = 1e-3, suffix = "K", accuracy = 1)) + 
#   scale_x_continuous(limits = c(1960, 2025), breaks = seq(1960, 2025, by = 10)) +  # x-axis from 0 to 100, broken by 10
#   labs(title = "Median Resale Price vs Remaining Lease Sold",
#        x = "Age Sold",
#        y = "Median Resale Price") +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1),  # Rotate x-axis labels for better readability
#         panel.grid.major.x = element_line(color = "black", size = 0.8),  # Darker lines on x-axis breaks
#         panel.grid.major.y = element_line(color = "black", size = 0.8)) +  # Darker lines on y-axis breaks
#   scale_alpha_continuous(range = c(0.1, 1))  # Set the range of alpha (opacity) from 0.1 (transparent) to 1 (opaque)
# 
# # There are dips only because the price are not adjusted for inflation
# 

