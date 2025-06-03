#include "postgres.h"
#include "omni_utils.h"

#include "parser/scansup.h"

/*
  将字符串按照指定分隔符分割成 List（基于 SplitIdentifierString 函数编写）。

  链表中的各个元素的内容是通过 nextp 指针直接在输入字符串 rawstring 上进行定位的，
  并没有使用动态内存分配函数来分配内存，因此，后面无需使用深拷贝释放这部分内存（强行释放会导致crash）。

  原 SplitIdentifierString 函数是用于处理SQL标识符（关键字）的，存在一些限制，不能直接使用：
  1. 默认会将空格及其他空白字符视为分隔符的一部分，因此，包含空格的子串必须以 "" 标注。
  2. 原函数用于分割 NameData 数据，会将长字符串截断为 64 个字符。
  3. 原函数会将字符串中的大写字母转换为小写。
 */
bool
split_string(char *rawstring, char separator, List **stringlist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*stringlist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !scanner_isspace(*nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */

		#if 0 // 原函数会截断 curname 为 64 字节，并转为小写，此处屏蔽
			int len = endp - curname;
			char *downname = downcase_truncate_identifier(curname, len, false);
			Assert(strlen(downname) <= len);
			strncpy(curname, downname, len);	/* strncpy is required here */
			pfree(downname);
		#endif
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

	#if 0 // 原函数会截断 curname 为 64 字节，此处屏蔽
		truncate_identifier(curname, strlen(curname), false);
	#endif

		/*
		 * Finished isolating current name --- add it to list
		 */
		*stringlist = lappend(*stringlist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}