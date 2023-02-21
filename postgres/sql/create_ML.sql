--SET client_min_messages TO WARNING;

LOAD :FACTML_LIBRARY;

CREATE OR REPLACE FUNCTION ridge_linear_regression(
        c cofactor, 
        label_idx int, 
        step_size float8, 
        lambda float8, 
        max_iterations int
    )
    RETURNS float8[]
    AS :FACTML_LIBRARY, 'ridge_linear_regression'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ridge_linear_regression_from_params(
        c float4[],
        label_idx int,
        step_size float8,
        lambda float8,
        max_iterations int
    )
    RETURNS float8[]
    AS :FACTML_LIBRARY, 'ridge_linear_regression_from_params'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

 CREATE OR REPLACE FUNCTION lda_train(
         c cofactor,
         label int,
         shrinkage float8
     )
     RETURNS float4[]
     AS :FACTML_LIBRARY, 'lda_train'
     LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

 CREATE OR REPLACE FUNCTION lda_train_from_params(
         c float4[],
         label_start int,
        label_end int,
         shrinkage float8
     )
     RETURNS float4[]
     AS :FACTML_LIBRARY, 'lda_train_from_params'
     LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

 CREATE OR REPLACE FUNCTION lda_impute(
         train_data float4[],
         feats_numerical float4[],
         feats_categorical int[]
     )
     RETURNS int
     AS :FACTML_LIBRARY, 'lda_impute'
     LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;