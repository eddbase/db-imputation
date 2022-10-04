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

CREATE OR REPLACE FUNCTION lda_train(
        c cofactor
    )
    RETURNS float4[]
    AS :FACTML_LIBRARY, 'lda_train'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lda_invert(
        cofactor float4[]
    )
    RETURNS float4[]
    AS :FACTML_LIBRARY, 'lda_invert'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lda_impute(
        means float4[],
        covariance float4[],
        probs_classes float4[],
        feats float4[]
    )
    RETURNS float4[]
    AS :FACTML_LIBRARY, 'lda_impute'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;