1. For each Stage implementation, evaluate for fatal errors (channel, etc) vs data issues or change 
return values accordingly; i.e., not just Ok(())
   1. see Tick as example 
   2. 1 test for continuation on the event of data issue; stop on the event of fatal error

1.