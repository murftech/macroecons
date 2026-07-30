source('/Users/murftech/Root/Prod/packages/precode-r/pre_runr.R')
library(scales)  # For formatting labels

runpre('pre_wrappers.R')

# data_2017 <- read_csv('data/govdata/Resale flat prices based on registration date from Jan-2017 onwards.csv')

if (!exists("data_2017")) {
  source('modules/pipe_hdb_old/fetch_hdb_data.R')
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


  
# Define 8 named filters for interactive chart
hdb_filters <- list(
  "Overall (no filter)"       = quote(TRUE),
  "Town: Bedok"               = quote(town == 'BEDOK'),
  "Town: Woodlands"           = quote(town == 'WOODLANDS'),
  "Town: Tampines"            = quote(town == 'TAMPINES'),
  "Tampines Target Streets"   = quote(street_name %in% c('TAMPINES AVE 7','TAMPINES ST 23','TAMPINES ST 24','TAMPINES ST 32','TAMPINES ST 42')),
  # "Tampines Ctrl 8"           = quote(street_name == 'TAMPINES CTRL 8'),
  "Tampines ST 22"            = quote(street_name == 'TAMPINES ST 22'),
  "Woodlands DR 16"           = quote(street_name == 'WOODLANDS DR 16'),
  "Jln Batu"                  = quote(str_detect(street_name, 'JLN BATU'))
)

hdb_sel <- hdbdata %>%
  select(tx_year, tx_monthdate, covid, flat_type, resale_price, age_sold, remaining_lease_sold, pretend_top_2025, street_name, storey_range, town) %>%
  filter(remaining_lease_sold <= 75) %>%
  filter(flat_type %in% c('4 ROOM')) %>%
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
  filter(tx_year <= 2023) %>%
  filter(tx_year==max(tx_year)) %>%
  rename(median_price_anchor = median_price) %>% select(-tx_year)

hdb_inflation <- hdb_median %>% left_join(locate_maxyear, by='flat_type') %>%
  mutate(inflation = median_price_anchor/median_price)
hdb_inflation <- hdb_inflation %>% select(tx_year, flat_type, inflation)
hdb_inflated <- hdb_sel %>% left_join(hdb_inflation) %>% mutate(resale_price_proxy_2025 = inflation*resale_price)

####################
print('interactive plotly: median price by filter selection')
####################

library(plotly)

# Pre-compute median + count for each of the 8 filters (from 2023 onwards)
filter_names <- names(hdb_filters)
n_filters    <- length(hdb_filters)
traces_per   <- 2  # line + bar per filter

all_medians <- list()
all_counts  <- list()

for (nm in filter_names) {
  sel <- hdb_sel %>% filter(tx_monthdate >= '2023-01-01') %>% filter(!!hdb_filters[[nm]])
  all_medians[[nm]] <- sel %>%
    group_by(tx_monthdate) %>%
    summarize(median_price = median(resale_price), .groups = 'drop') %>%
    arrange(tx_monthdate)
  all_counts[[nm]] <- sel %>%
    group_by(tx_monthdate) %>%
    summarise(count = n(), .groups = 'drop') %>%
    arrange(tx_monthdate)
}

# Global y-axis scale (consistent across all filters)
all_prices   <- bind_rows(all_medians) %>% pull(median_price)
all_cnts     <- bind_rows(all_counts)  %>% pull(count)
scale_min    <- floor(min(all_prices, na.rm = TRUE) / 10000) * 10000
scale_max    <- ceiling(max(all_prices, na.rm = TRUE) / 10000) * 10000
scale_factor <- (scale_max - scale_min) / max(all_cnts, na.rm = TRUE)

# Build plotly figure with one line + one bar per filter
fig <- plot_ly()

for (i in seq_along(filter_names)) {
  nm      <- filter_names[i]
  med     <- all_medians[[nm]]
  cnt     <- all_counts[[nm]]
  visible <- (i == 1)

  # Median price line
  fig <- fig %>% add_trace(
    x          = med$tx_monthdate,
    y          = med$median_price,
    type       = 'scatter',
    mode       = 'lines+markers+text',
    name       = nm,
    text       = paste0(round(med$median_price / 1000), "K"),
    textposition = 'top center',
    line       = list(color = 'steelblue', width = 2),
    marker     = list(color = 'steelblue', size = 6),
    visible    = visible,
    showlegend = FALSE
  )

  # Transaction count bars (scaled to same y-axis)
  fig <- fig %>% add_trace(
    x          = cnt$tx_monthdate,
    y          = cnt$count * scale_factor + scale_min,
    type       = 'bar',
    name       = paste0(nm, " (n)"),
    text       = cnt$count,
    textposition = 'inside',
    insidetextanchor = 'start',
    marker     = list(color = 'rgba(0,0,0,0.25)'),
    visible    = visible,
    showlegend = FALSE
  )
}

# Dropdown buttons — each toggles one filter's pair of traces
buttons <- lapply(seq_along(filter_names), function(i) {
  vis <- rep(FALSE, n_filters * traces_per)
  vis[((i - 1) * traces_per + 1):(i * traces_per)] <- TRUE
  list(method = "restyle",
       args   = list("visible", as.list(vis)),
       label  = filter_names[i])
})

fig <- fig %>% layout(
  title  = "HDB 4-Room Resale Prices (2023+) — Median & Volume",
  xaxis  = list(title = "Transaction Month"),
  yaxis  = list(title = "Resale Price (SGD)",
                range  = c(scale_min, 700000),
                tickformat = ",d"),
  barmode = "overlay",
  updatemenus = list(
    list(
      type      = "dropdown",
      active    = 0,
      buttons   = buttons,
      x         = 0,
      y         = 1.18,
      xanchor   = "left",
      yanchor   = "top",
      bgcolor   = "#f0f0f0",
      bordercolor = "#999"
    )
  )
)

fig

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

