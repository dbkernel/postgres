#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*--------------------------------*
 *  文本转任意类型的通用转换函数  *
 *--------------------------------*/
PG_FUNCTION_INFO_V1(text_to_type);
Datum
text_to_type(PG_FUNCTION_ARGS)
{
    text       *text_arg = PG_GETARG_TEXT_PP(0);
    Oid         target_type;
    int32       typmod = -1;
    Oid         typinput;
    Oid         typioparam;
    char       *str;
    Datum       result;

    // TODO: 当前函数尚未调通，执行时返回一行，但结果为空

    /* 获取目标类型和类型修饰符 */
    target_type = get_fn_expr_argtype(fcinfo->flinfo, 1); // 从第二个参数获取类型

    // 根据目标类型设置 typmod
    // if (target_type == VARCHAROID) {
    //     typmod = 255; // 对于 varchar，设置最大长度
    // } else if (target_type == CHAROID) {
    //     typmod = 128; // 假设我们处理 char(128)
    // } else if (target_type == NUMERICOID) {
    //     typmod = (int32) (numeric_get_typmod(fcinfo->args[1])); // 从参数中获取 numeric 的 typmod
    // }

    if (!OidIsValid(target_type))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("The target type cannot be inferred"))); // 无法推断目标类型

    getTypeInputInfo(target_type, &typinput, &typioparam);
    str = text_to_cstring(text_arg);
    elog(WARNING, "input: %s", str);
    result = OidInputFunctionCall(typinput, str, typioparam, typmod);

    PG_RETURN_DATUM(result);
}

/*--------------------------------*
 *  任意类型转文本的通用转换函数  *
 *--------------------------------*/
PG_FUNCTION_INFO_V1(type_to_text);
Datum
type_to_text(PG_FUNCTION_ARGS)
{
    Datum       value = PG_GETARG_DATUM(0);
    Oid         val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
    Oid         typoutput;
    bool        typisvarlena;
    char       *str;

    /* 获取目标类型的输出函数 */
    getTypeOutputInfo(val_type, &typoutput, &typisvarlena);

    /* 调用类型输出函数 */
    str = OidOutputFunctionCall(typoutput, value);

    PG_RETURN_TEXT_P(cstring_to_text(str));
}