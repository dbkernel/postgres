/*-------------------------------------------------------------------------
 *
 * seal_example.h
 *	  definition of the "access method operator" system catalog
 *(seal_example)
 *
 * src/include/catalog/seal_example.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEAL_EXAMPLE_H
#define SEAL_EXAMPLE_H

#include "catalog/seal_example_d.h"
#include "catalog/genbki.h"

/* ----------------
 *		seal_example definition.  cpp turns this into
 *		typedef struct FormData_seal_example
 * ----------------
 */
CATALOG(seal_example,8200,SealExampleRelationId) {
  Oid oid; /* get by executing ./unused_oids in src/include/catalog/ */
  int32 number BKI_DEFAULT(0);
  text tname BKI_DEFAULT(_null_);
}
FormData_seal_example;

/* ----------------
 *		Form_seal_example corresponds to a pointer to a tuple with
 *		the format of seal_example relation.
 * ----------------
 */
typedef FormData_seal_example *Form_seal_example;

/* SysCache 缓存需要使用唯一索引（支持单列索引或最多4列的多列索引），其在 cacheinfo[] 中注册 */
DECLARE_UNIQUE_INDEX_PKEY(seal_example_oid_index, 8201, SealExampleOidIndexId, on seal_example using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(seal_example_number_index, 8202, SealExampleNumberIndexId, on seal_example using btree(number int4_ops));

extern void InsertSealExampleTuple(int32 number, char *tname);
extern void UpdateSealExampleTupleByTableScan(int32 number, int32 new_number);
extern void DeleteSealExampleTupleByTableScan(int32 number);
extern void UpdateSealExampleTupleByCacheLookup(int32 number, int32 new_number);
extern void DeleteSealExampleTupleByCacheLookup(int32 number);
extern Form_seal_example SearchSealExampleTuple(int32 number);

#endif /* SEAL_EXAMPLE_H */