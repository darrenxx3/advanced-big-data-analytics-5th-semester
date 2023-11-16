session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("YVA285", "MVAINJURIES00");
if exists0 == 0 then do;
  print "Table "||"YVA285"||"."||"MVAINJURIES00" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"YVA285"||"."||"MVAINJURIES00"||" found.";
exists1=doesTableExist("YVA285", "MVAINJURIES90");
if exists1 == 0 then do;
  print "Table "||"YVA285"||"."||"MVAINJURIES90" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"YVA285"||"."||"MVAINJURIES90"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="MVAINJURIES90";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="a528f86b-04bf-4b3d-b8a3-a4552f6511e0";
  _dp_outputCaslib="YVA285";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "a528f86b-04bf-4b3d-b8a3-a4552f6511e0" (caslib="YVA285" promote="no");


    /* Set the input                                                                set */
    set
        "MVAINJURIES90" (caslib="YVA285")
        "MVAINJURIES00" (caslib="YVA285")
        ;

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="a528f86b-04bf-4b3d-b8a3-a4552f6511e0";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="a528f86b-04bf-4b3d-b8a3-a4552f6511e0";
  _dp_outputCaslib="YVA285";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "a528f86b-04bf-4b3d-b8a3-a4552f6511e0" (caslib="YVA285" promote="no");

    length
       "DVRatio"n 8
    ;

    /* Set the input                                                                set */
    set "a528f86b-04bf-4b3d-b8a3-a4552f6511e0" (caslib="YVA285"  );

    /* BEGIN statement A33FCBC5_DEE1_4E48_A842_8A44B7B6391C                  calccolumn */
    "DVRatio"n = Drivers/Vehicles;
    /* END statement A33FCBC5_DEE1_4E48_A842_8A44B7B6391C                    calccolumn */

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="a528f86b-04bf-4b3d-b8a3-a4552f6511e0";
  _dp_inputCaslib="YVA285";

  _dp_outputTable="MVAINJURIES90_NEW";
  _dp_outputCaslib="YVA285";

srcCasTable="a528f86b-04bf-4b3d-b8a3-a4552f6511e0";
srcCasLib="YVA285";
tgtCasTable="MVAINJURIES90_NEW";
tgtCasLib="YVA285";
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


dropTableIfExists("YVA285", "a528f86b-04bf-4b3d-b8a3-a4552f6511e0");

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
