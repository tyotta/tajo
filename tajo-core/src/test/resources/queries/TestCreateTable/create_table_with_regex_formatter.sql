CREATE EXTERNAL TABLE tableWithRegexFormatter (id text, str text, other text) using csv with('csvfile.regexFormatter'='([^\\s]+) ([^\\s]+) (.*)') location ${table.path};