
showtime <- function() 
{
  # Get the current time
  current_time <- Sys.time()
  
  # Format the current time as "HH:MM:SS"
  time_str <- format(current_time, "%H:%M:%S")
  
  # Print the formatted time
  # print(time_str)
  
  # Return the formatted time
  return(time_str)
}


# runr('abc')
require(tidyverse)
runr <- function(file_path) {
  sprintf('SCRIPT RUN STARTED!!!!!!: %s', file_path) %>% print()
  sprintf('script started time: %s', showtime()) %>% print()
  source(file_path)
  sprintf('SCRIPT HAS FINISHED!!!!!!: %s', file_path) %>% print()
  sprintf('script finished time: %s', showtime()) %>% print()
}

runpre <- function(precode_path, precode_repo = 'packages/precode-r') 
{
  file_path = file.path(precode_repo, precode_path)
  sprintf('sourcing %s', file_path) %>% print()
  source(file_path)
  sprintf('%s precode loaded', file_path) %>% print()
}


# def precodepull(file_path):
#     cdsw_path = f'/home/cdsw/{file_path}'
#     exec(open(cdsw_path).read(), globals())
#     print(cdsw_path + 'precode loaded')

