library(shiny)
#library(DT)
#library(plotly)
library(dygraphs)

#project_path = '/Users/sofiasilva/GitHub/cool_bgp_stats'
shiny_app_path = paste(c(project_path, '/shiny_cool_bgp_stats/'), collapse='')
#load(paste(c(shiny_app_path, '/delegated.RData'), collapse=''))

delegated_stats_file = '/Users/sofiasilva/BGP_files/delegated_stats_20170126.csv'
del_stats = read.csv2(delegated_stats_file, header = F, sep = ',')

shinyUI(fluidPage(
  
  # Application title
  titlePanel("Cool BGP Stats"),
  
  # Sidebar with a slider input for the number of bins
  sidebarLayout(
    #TODO Poner tabs para mostrar estadisticas a nivel de region y luego por pais
    #TODO a nivel de region, usar choropleths para resumir estadisticas de paises
    sidebarPanel(
      selectInput('countrySelect', "Choose country",
                  choices = list("United Arab Emirates" = "AE", "Afghanistan" = "AF", "Asia Pacific" = "AP", "American Samoa" = "AS", "Australia" = "AU", "Bangladesh" = "BD", "Brunei Darussalam" = "BN", "Brazil" = "BR", "Bahamas" = "BS", "Bhutan" = "BT", "Belize" = "BZ", "Canada" = "CA", "Cook Islands" = "CK", "China" = "CN", "Colombia" = "CO", "Germany" = "DE", "Spain" = "ES", "Fiji" = "FJ", "Micronesia, Federated States of" = "FM", "France" = "FR", "United Kingdom" = "GB", "Guam" = "GU", "Hong Kong" = "HK", "Indonesia" = "ID", "Isle of Man" = "IM", "India" = "IN", "British Indian Ocean Territory" = "IO", "Iran, Islamic Republic of" = "IR", "Japan" = "JP", "Kenya" = "KE", "Cambodia" = "KH", "Kiribati" = "KI", "Korea, Democratic People's Republic of" = "KP", "Korea, Republic of" = "KR", "Lao People's Democratic Republic" = "LA", "Sri Lanka" = "LK", "Marshall Islands" = "MH", "Myanmar" = "MM", "Mongolia" = "MN", "Macao" = "MO", "Northern Mariana Islands" = "MP", "Mauritius" = "MU", "Maldives" = "MV", "Malaysia" = "MY", "New Caledonia" = "NC", "Norfolk Island" = "NF", "Netherlands" = "NL", "Norway" = "NO", "Nepal" = "NP", "Nauru" = "NR", "Niue" = "NU", "New Zealand" = "NZ", "French Polynesia" = "PF", "Papua New Guinea" = "PG", "Philippines" = "PH", "Pakistan" = "PK", "Palau" = "PW", "Saudi Arabia" = "SA", "Solomon Islands" = "SB", "Seychelles" = "SC", "Sweden" = "SE", "Singapore" = "SG", "Thailand" = "TH", "Tokelau" = "TK", "Timor-Leste" = "TL", "Tonga" = "TO", "Turkey" = "TR", "Tuvalu" = "TV", "Taiwan, Province of China" = "TW", "United States" = "US", "Virgin Islands, British" = "VG", "Viet Nam" = "VN", "Vanuatu" = "VU", "Wallis and Futuna" = "WF", "Samoa" = "WS", "South Africa" = "ZA")),
      #tags$em("Select the country you're interested in."),
      radioButtons("radioResourceType", "Choose Type of Resource", 
                   choices = list("ASN" = "asn", "IPv4" = "ipv4", "IPv6" = "ipv6"),
                   selected = 'ipv4'),
      #tags$em("Select the type of resource you're interested in."),
      #TODO ver como mostrar estos check boxes solo si el resource type es IPv4 o IPv6
      checkboxGroupInput('assig_or_alloc', "Choose type of delegation",
                         choices = list("Assignment" = 'assigned',
                                        "Allocation" = 'allocated'),
                         selected = c('assigned', 'allocated'))
      #tags$em("Select the ...")
      #TODO poner dateRangeInput para seleccionar rango de fechas
      #TODO poner slider para seleccionar modularidad (d??a, semana, mes, a??o)
    ),
    mainPanel(
      plotOutput("resCount")
    )
  )
))
