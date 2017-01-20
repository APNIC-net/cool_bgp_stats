library(countrycode)
library(rjstat)

#today <- gsub("-","", Sys.Date())
today = '20170118'

bgp_path = '/Users/sofiasilva/GitHub/cool_bgp_stats'
shiny_app_path = paste(c(bgp_path, '/shiny_cool_bgp_stats'), sep='', collapse='')

dest_file_delegated = paste(c(bgp_path, '/BGP_files/delegated_apnic_', today, '.txt'), sep='', collapse = '')
#download.file('https://ftp.apnic.net/stats/apnic/delegated-apnic-latest',
#              dest_file_delegated)

#dest_file_extended = paste(c(bgp_path, '/BGP_files/delegated_apnic_extended_', today, '.txt'), sep='', collapse='')
#download.file('https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest',
#              dest_file_extended)

delegated_df = read.csv2(dest_file_delegated, header = F, sep = '|', comment.char = '#',
                         col.names = c('registry', 'cc', 'resource_type', 'initial_resource', 'count', 'date', 'alloc_assig'))

res_types = c('All', 'asn', 'ipv4', 'ipv6')
del_types = c('All', 'allocated', 'assigned')
granularities = c('All', 'daily', 'weekly', 'monthly', 'annually')
aggr_formula_granularities = c(NA, 'day + week + month + year', 'week + month + year', 'month + year', 'year')


original_records = data.frame(Type=character(), count=integer())
for(i in seq(1,4)){
  if(i == 1){
    aux_df = data.frame(Type=res_types[i], count=as.integer(as.character(delegated_df[i,]$initial_resource)))
  }else{
    aux_df = data.frame(Type=res_types[i], count=as.integer(as.character(delegated_df[i,]$count)))
  }
  original_records = rbind(original_records, aux_df)
}

delegated_df$initial_resource = as.character(delegated_df$initial_resource)
keep = as.integer(delegated_df[1,]$initial_resource)
delegated_df = delegated_df[seq(nrow(delegated_df)-keep+1, nrow(delegated_df)),]

CCs = as.factor(c('All', as.character(unique(delegated_df$cc))))

#delegated_df = separate(data = delegated_df, col = 'date', into = c('year', 'month', 'day'), sep = c(4, 6), remove = F)
delegated_df$date = as.Date(delegated_df$date, format='%Y%m%d')
delegated_df$year = as.numeric( format( as.Date(cut(delegated_df$date, breaks = "year")), '%Y%m%d'))
delegated_df$month <- as.numeric( format( as.Date(cut(delegated_df$date, breaks = "month")), '%Y%m%d'))
delegated_df$week <- as.numeric( format( as.Date(cut(delegated_df$date, breaks = "week", start.on.monday = T)), '%Y%m%d%W'))
delegated_df$day <- as.numeric( format( as.Date(cut(delegated_df$date, breaks = "day")), '%Y%m%d'))

#save(delegated_df, file=paste(c(shiny_app_path, '/delegated.RData'), sep='', collapse=''))

# For testing
#delegated_df = delegated_df[c(1:20, 40000:40030, 49000:49030),]

# CCs_to_convert = sort(unique(delegated_df$cc))
# APNIC_countries_df = data.frame(Country_name = countrycode(CCs_to_convert, origin='iso2c', destination = 'country.name'), CC=CCs_to_convert)
# 
# APNIC_countries_df$Country_name = as.character(APNIC_countries_df$Country_name)
# APNIC_countries_df[APNIC_countries_df$CC == 'AP',]$Country_name = 'Asia Pacific'
# APNIC_countries_df$Country_name = as.factor(APNIC_countries_df$Country_name)
# 
# write.csv2(APNIC_countries_df, file=paste(c(shiny_app_path, '/country_names.txt'), sep='', collapse = ''), row.names = F)


countries_loop <- function(res_df, g, r, d, stats_df) {
  for(c in CCs){
    if(c == 'All'){
      country_res_df = res_df
    }else{
      country_res_df = subset(res_df, res_df$cc == c)
    }
    
    if(g == 1){
      aux_df = data.frame(CC=c, ResourceType=r, DelegationType=d, Granularity=granularities[g], Day='NA', Week='NA', Month='NA', Year='NA', value=nrow(country_res_df), stringsAsFactors = F)
      stats_df = rbind(stats_df, aux_df)
    }else{
      
      if(nrow(country_res_df) != 0){
        aggr = aggregate(as.formula(paste(c('registry ~ ', aggr_formula_granularities[g]), collapse='')), data=country_res_df, FUN = NROW)
        l = nrow(aggr)
        
        days = rep('NA', l)
        days[!is.null(aggr$day)] = aggr$day[!is.null(aggr$day)]
        
        weeks = rep('NA', l)
        weeks[!is.null(aggr$week)] = aggr$week[!is.null(aggr$week)]
        
        months = rep('NA', l)
        months[!is.null(aggr$month)] = aggr$month[!is.null(aggr$month)]
        
        years = rep('NA', l)
        years[!is.null(aggr$year)] = aggr$year[!is.null(aggr$year)]
        
        aux_df = data.frame(CC=c, ResourceType=r, DelegationType=d, Granularity=granularities[g], Day=days, Week=weeks, Month=months, Year=years, value=aggr$registry, stringsAsFactors = F)
        stats_df = rbind(stats_df, aux_df)
      }
    }
  }
  return(stats_df)
}

# Just to verify
num_rows = nrow(delegated_df)
if(num_rows != original_records[original_records$Type == 'All',]$count){
  print('THERE\'S SOMETHING WRONG!')
}

for(r in res_types[2:4]){
  total = nrow(subset(delegated_df, delegated_df$resource_type == r))
  if(total != original_records[original_records$Type == r,]$count){
    print(paste('THERE\'S SOMETHING WRONG WITH THE NUMBER OF ', r))
  }
}

stats_df = data.frame(CC=character(), ResourceType=character(), DelegationType=character(), Granularity=character(), Day=character(), Week=character(), Month=character(), Year=character(), value=integer(), stringsAsFactors = F)


for(g in seq(1,5)){
  for(r in res_types){
    if(r == 'All'){
      res_df = delegated_df
    }else{
      res_df = subset(delegated_df, delegated_df$resource_type == r)
    }
    
    if(r == 'ipv4' | r == 'ipv6'){
      for(d in del_types){
        if(d == 'All'){
          del_res_df = res_df
        }else{
          del_res_df = subset(res_df, res_df$alloc_assig == d)
        }
        stats_df = countries_loop(del_res_df, g, r, d, stats_df)
      }
    }else{
      stats_df = countries_loop(res_df, g, r, 'All', stats_df)  
    }
  }
}

write.csv2(stats_df, file=paste(c(bgp_path, '/BGP_files/delegated_stats.csv'), sep='', collapse = ''), row.names = T)


JSONstat <- toJSONstat(stats_df)
write(JSONstat, file=paste(c(bgp_path, "/BGP_files/delegated_stats.json"), collapse=''))


#TODO luego de tener las estadisticas hasta el momento, tengo que escribir codigo
# que solo lea lo del ultimo mes y lo agregue al data frame de estadisticas
# y luego genere el nuevo JSON
# actualizar las estadisticas existentes que cambien ('All')

# Como referencia
# stats_df_forJSON = stats_df
# 
# for(i in seq(5, 8)){
#   stats_df_forJSON[i] = lapply(stats_df_forJSON[i], as.character)
#   stats_df_forJSON[i] = replace(stats_df_forJSON[i], is.na(stats_df_forJSON[i]), 'NA')
# }