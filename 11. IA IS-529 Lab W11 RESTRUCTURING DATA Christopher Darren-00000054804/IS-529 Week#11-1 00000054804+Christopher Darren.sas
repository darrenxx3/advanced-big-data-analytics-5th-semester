session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("CASUSER(christopher.darren@student.umn.ac.id)", "SALES_ORD_T");
if exists0 == 0 then do;
  print "Table "||"CASUSER(christopher.darren@student.umn.ac.id)"||"."||"SALES_ORD_T" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"CASUSER(christopher.darren@student.umn.ac.id)"||"."||"SALES_ORD_T"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="SALES_ORD_T";
  _dp_inputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

  _dp_outputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_outputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

transpose.transpose
    table = {name="SALES_ORD_T" caslib="CASUSER(christopher.darren@student.umn.ac.id)"}
    casout = {replace=1, name="1d44bcd8-1846-41bc-8c5b-fdcd70430410" caslib="CASUSER(christopher.darren@student.umn.ac.id)"}
    id = {"Measure"}
    let = 0
    transpose = {"JAN2014", "JAN2015", "JAN2016", "JAN2017", "FEB2014", "FEB2015", "FEB2016", "FEB2017", "MAR2014", "MAR2015", "MAR2016", "MAR2017", "APR2014", "APR2015", "APR2016", "APR2017", "MAY2014", "MAY2015", "MAY2016", "MAY2017", "JUN2014", "JUN2015", "JUN2016", "JUN2017", "JUL2014", "JUL2015", "JUL2016", "JUL2017", "AUG2014", "AUG2015", "AUG2016", "AUG2017", "SEP2014", "SEP2015", "SEP2016", "SEP2017", "OCT2014", "OCT2015", "OCT2016", "NOV2014", "NOV2015", "NOV2016", "DEC2013", "DEC2014", "DEC2015", "DEC2016"};

/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  print "Error running the transpose action.  Status code: " || _status.statusCode;
  exit 1;
end;
  _dp_inputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_inputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

  _dp_outputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_outputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "1d44bcd8-1846-41bc-8c5b-fdcd70430410" (caslib="CASUSER(christopher.darren@student.umn.ac.id)" promote="no");

    length
       "Date"n $ 8
       "TransactionDate"n 8
    ;
    label
        "TransactionDate"n="Transaction Date"
    ;
    format
        "Date"n 8.
        "TransactionDate"n DATE9.
    ;

    /* Set the input                                                                set */
    set "1d44bcd8-1846-41bc-8c5b-fdcd70430410" (caslib="CASUSER(christopher.darren@student.umn.ac.id)"   rename=("_NAME_"n = "Date"n) );

    /* BEGIN statement_5b9646b5_bd31_41ab_b009_981ed56daa19              convert_column */
    "TransactionDate"n= INPUT(strip("Date"n),MONYY7.);
    /* END statement_5b9646b5_bd31_41ab_b009_981ed56daa19                convert_column */

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_inputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

  _dp_outputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_outputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "1d44bcd8-1846-41bc-8c5b-fdcd70430410" (caslib="CASUSER(christopher.darren@student.umn.ac.id)" promote="no");


    /* Set the input                                                                set */
    set "1d44bcd8-1846-41bc-8c5b-fdcd70430410" (caslib="CASUSER(christopher.darren@student.umn.ac.id)"   drop="Date"n );

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
  _dp_inputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

  _dp_outputTable="SALES_ORD_T_NEW";
  _dp_outputCaslib="CASUSER(christopher.darren@student.umn.ac.id)";

srcCasTable="1d44bcd8-1846-41bc-8c5b-fdcd70430410";
srcCasLib="CASUSER(christopher.darren@student.umn.ac.id)";
tgtCasTable="SALES_ORD_T_NEW";
tgtCasLib="CASUSER(christopher.darren@student.umn.ac.id)";
saveType="sashdat";
tgtCasTableLabel="";
replace=1;
saveToDisk=1;

exists = doesTableExist(tgtCasLib, tgtCasTable);
if (exists !=0) then do;
  if (replace == 0) then do;
    print "Table already exists and replace flag is set to false.";
    exit ({severity=2, reason=5, formatted="Table already exists and replace flag is set to false.", statusCode=9});
  end;
end;

if (saveToDisk == 1) then do;
  /* Save will automatically save as type represented by file ext */
  saveName=tgtCasTable;
  if(saveType != "") then do;
    saveName=tgtCasTable || "." || saveType;
  end;
  table.save result=r status=rc / caslib=tgtCasLib name=saveName replace=replace
    table={
      caslib=srcCasLib
      name=srcCasTable
    };
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
  tgtCasPath=dictionary(r, "name");

  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);

  table.loadtable result=r status=rc / caslib=tgtCasLib path=tgtCasPath casout={name=tgtCasTable caslib=tgtCasLib} promote=1;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;

else do;
  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);
  table.promote result=r status=rc / caslib=srcCasLib name=srcCasTable target=tgtCasTable targetLib=tgtCasLib;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;


dropTableIfExists("CASUSER(christopher.darren@student.umn.ac.id)", "1d44bcd8-1846-41bc-8c5b-fdcd70430410");

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
