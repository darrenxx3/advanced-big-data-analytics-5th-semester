session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("YVA285", "NA_HURRICANES");
if exists0 == 0 then do;
  print "Table "||"YVA285"||"."||"NA_HURRICANES" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"YVA285"||"."||"NA_HURRICANES"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="NA_HURRICANES";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="60fbf7ac-c1ef-48e1-ad00-028ac4765ea3";
  _dp_outputCaslib="YVA285";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "60fbf7ac-c1ef-48e1-ad00-028ac4765ea3" (caslib="YVA285" promote="no");

    length
       "from_loc"n $ 400
       "from_lat_long"n $ 400
       "from_latitude"n $ 20
       "from_longitude"n $ 20
       "from_lat"n 8
       "from_lon"n 8
    ;
    format
        "from_lat"n BEST16.
        "from_lon"n BEST16.
    ;

    /* Set the input                                                                set */
    set "NA_HURRICANES" (caslib="YVA285"  );

    /* BEGIN statement 1CD581BC_155E_46F4_AAD4_89AD8E962788                       split */
    drop temp_89AD8E962788;
    temp_89AD8E962788 = kindexc ("From"n, ":");
    if temp_89AD8E962788 <= 0 then do;
       "from_loc"n = "From"n;
       "from_lat_long"n = "";
    end;
    else do;
    /* Create left side of split                                                        */
        "from_loc"n=ksubstr("From"n, 1, temp_89AD8E962788 - 1);
    /* Create right side of split                                                       */
        "from_lat_long"n = ksubstr("From"n, temp_89AD8E962788 + 1);
    end;
    /* END statement 1CD581BC_155E_46F4_AAD4_89AD8E962788                         split */

    /* BEGIN statement 1CD581BC_155E_46F4_AAD4_89AD8E962788                       split */
    drop temp_89AD8E962788;
    temp_89AD8E962788 = kindexc ("from_lat_long"n, ",");
    if temp_89AD8E962788 <= 0 then do;
       "from_latitude"n = "from_lat_long"n;
       "from_longitude"n = "";
    end;
    else do;
    /* Create left side of split                                                        */
        "from_latitude"n=ksubstr("from_lat_long"n, 1, temp_89AD8E962788 - 1);
    /* Create right side of split                                                       */
        "from_longitude"n = ksubstr("from_lat_long"n, temp_89AD8E962788 + 1);
    end;
    /* END statement 1CD581BC_155E_46F4_AAD4_89AD8E962788                         split */

    /* BEGIN statement_5b9646b5_bd31_41ab_b009_981ed56daa19              convert_column */
    "from_lat"n= INPUT(strip("from_latitude"n),BEST16.);
    /* END statement_5b9646b5_bd31_41ab_b009_981ed56daa19                convert_column */

    /* BEGIN statement_5b9646b5_bd31_41ab_b009_981ed56daa19              convert_column */
    "from_lon"n= INPUT(strip("from_longitude"n),BEST16.);
    /* END statement_5b9646b5_bd31_41ab_b009_981ed56daa19                convert_column */

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="60fbf7ac-c1ef-48e1-ad00-028ac4765ea3";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="60fbf7ac-c1ef-48e1-ad00-028ac4765ea3";
  _dp_outputCaslib="YVA285";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "60fbf7ac-c1ef-48e1-ad00-028ac4765ea3" (caslib="YVA285" promote="no");


    /* Set the input                                                                set */
    set "60fbf7ac-c1ef-48e1-ad00-028ac4765ea3" (caslib="YVA285"   drop="From"n  drop="from_lat_long"n  drop="from_latitude"n  drop="from_longitude"n );

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="60fbf7ac-c1ef-48e1-ad00-028ac4765ea3";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="NA_HURRICANES_1";
  _dp_outputCaslib="YVA285";

/* BEGIN statement 036623f2_eefe_11e6_bc64_92361f002671          datastep_statement */
dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table data */
data "NA_HURRICANES_1" (caslib="YVA285" promote="no");
/* Set the input set */
set "60fbf7ac-c1ef-48e1-ad00-028ac4765ea3" (caslib="YVA285" );
length Category $15;
if Type = ''HU'' then Category=''Hurricane'';
else if Type in (''TD'' ''TS'' ''WV'') then Category=''Tropical'';
else if Type in (''SS'' ''SD'') then Category=''Subtropical'';
else if Type=''EX'' then Category=''Extratropical'';
else Category=''Other'';
run;';
if rc.statusCode != 0 then do;
  print "Error executing datastep statement in CASL";
  exit 3;
end;
/* END statement 036623f2_eefe_11e6_bc64_92361f002671            datastep_statement */


dropTableIfExists("YVA285", "60fbf7ac-c1ef-48e1-ad00-028ac4765ea3");

function doesTableExist(casLib, casTable);
  table.tableExists result=r status=rc / caslib=casLib table=casTable;
  tableExists = dictionary(r, "exists");
  return tableExists;
end func;

function dropTableIfExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists != 0 then do;
    print "Dropping table: "||casLib||"."||casTable;
    table.dropTable result=r status=rc/ caslib=casLib table=casTable quiet=0;
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;

/* Return list of columns in a table */
function columnList(casLib, casTable);
  table.columnInfo result=collist / table={caslib=casLib,name=casTable};
  ndimen=dim(collist['columninfo']);
  featurelist={};
  do i =  1 to ndimen;
    featurelist[i]=upcase(collist['columninfo'][i][1]);
  end;
  return featurelist;
end func;
