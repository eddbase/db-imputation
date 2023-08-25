#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SHARED_FILE=`ls $SCRIPT_DIR/../lib/libfactML.* 2> /dev/null`

if [ -z "$SHARED_FILE" ]
then
    echo "Shared library is missing. Need to recompile."
else
    echo "Creating relation type..."
    psql -f $SCRIPT_DIR/create_relation_type.sql -v FACTML_LIBRARY=\'$SHARED_FILE\' postgres
    
    echo "Creating Naive Bayes type..."
    psql -f $SCRIPT_DIR/create_nb_type.sql -v FACTML_LIBRARY=\'$SHARED_FILE\' postgres

    
    echo "Creating cofactor type..."
    psql -f $SCRIPT_DIR/create_cofactor_type.sql -v FACTML_LIBRARY=\'$SHARED_FILE\' postgres

    echo "Creating ML functions..."
    psql -f $SCRIPT_DIR/create_ML.sql -v FACTML_LIBRARY=\'$SHARED_FILE\' postgres
fi
