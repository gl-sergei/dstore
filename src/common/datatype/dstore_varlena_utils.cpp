/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_varlena_utils.cpp
 *
 * IDENTIFICATION
 *        dstore/src/common/datatype/dstore_varlena_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <clocale>
#include "common/datatype/dstore_varlena_utils.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "catalog/dstore_typecache.h"


namespace DSTORE {

/*
 * Detect whether collation's LC_COLLATE property is C
 */
bool LcCollateIsC(Oid collation)
{
    /*
     * If we're asked about "collation 0", return false, so that the code will
     * go into the non-C path and report that the collation is bogus.
     */
    if (!DstoreOidIsValid(collation)) {
        return false;
    }

    /*
     * If we're asked about the default collation, we have to inquire of the C
     * library.  Cache the result so we only have to compute it once.
     */
    if (collation == DEFAULT_COLLATION_OID) {
        char *localeptr = setlocale(LC_COLLATE, nullptr);
        if (!localeptr) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid LC_COLLATE setting"));
            return false;
        }

        if (strcmp(localeptr, "C") == 0) {
            return true;
        } else if (strcmp(localeptr, "POSIX") == 0) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * If we're asked about the built-in C/POSIX collations, we know that.
     */
    if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID) {
        return true;
    }

    /*
     * Otherwise, we have to consult pg_collation, but we cache that.
     */
    return false;
}

/* VarstrCmp()
 * Comparison function for text strings with given lengths.
 * Includes locale support, but must copy strings to temporary memory
 * to allow null-termination for inputs to strcoll().
 * Returns an integer less than, equal to, or greater than zero, indicating
 * whether arg1 is less than, equal to, or greater than arg2.
 */
int VarstrCmp(char* arg1, int len1, char* arg2, int len2, Oid collid)
{
    int result;

    /*
     * Unfortunately, there is no strncoll(), so in the non-C locale case we
     * have to do some memory copying. This turns out to be significantly
     * slower, so we optimize the case where LC_COLLATE is C.  We also try to
     * optimize relatively-short strings by avoiding palloc/pfree overhead.
     */
    if (LcCollateIsC(collid)) {
        result = memcmp(arg1, arg2, static_cast<uint32>(DstoreMin(len1, len2)));
        if ((result == 0) && (len1 != len2)) {
            result = (len1 < len2) ? -1 : 1;
        }
    } else {
        char a1buf[TEXTBUFLEN];
        char a2buf[TEXTBUFLEN];
        char *a1p = nullptr, *a2p = nullptr;

#ifdef HAVE_LOCALE_T
        pg_locale_t mylocale = 0;
#endif

        if (collid != DEFAULT_COLLATION_OID) {
            if (!DstoreOidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                storage_set_error(DATATYPE_ERROR_INDETERMINATE_COLLATION);
            }
#ifdef HAVE_LOCALE_T
            mylocale = pg_newlocale_from_collation(collid);
#endif
        }

        /*
         * memcmp() can't tell us which of two unequal strings is sorted first, but
         * it's a cheap way to tell if they're equal.  Testing shows that
         * memcmp() followed by strcoll() is only trivially slower than
         * strcoll() by itself, so we don't have much to lose if this doesn't happen
         * often, if it happens - for example, because there are many
         * equal strings in the input - then we win big by avoiding expensive
         * collation-aware comparisons.
         */
        bool isEqual = len1 == len2 && memcmp(arg1, arg2, static_cast<uint32>(len1)) == 0;
        if (isEqual) {
            return 0;
        }

        if (len1 >= TEXTBUFLEN) {
            a1p = static_cast<char *>(DstorePalloc(static_cast<uint32>(len1 + 1)));
        } else {
            a1p = a1buf;
        }
        if (len2 >= TEXTBUFLEN) {
            a2p = static_cast<char *>(DstorePalloc(static_cast<uint32>(len2 + 1)));
        } else {
            a2p = a2buf;
        }
        if (STORAGE_VAR_NULL(a1p) || STORAGE_VAR_NULL(a2p)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            return -1;
        }

        errno_t err = EOK;
        if (len1 > 0) {
            err = memcpy_s(a1p, static_cast<uint32>(len1), arg1, static_cast<uint32>(len1));
            storage_securec_check(err, "\0", "\0");
        }
        a1p[len1] = '\0';
        if (len2 > 0) {
            err = memcpy_s(a2p, static_cast<uint32>(len2), arg2, static_cast<uint32>(len2));
            storage_securec_check(err, "\0", "\0");
        }
        a2p[len2] = '\0';

#ifdef HAVE_LOCALE_T
        result = mylocale ? strcoll_l(a1p, a2p, mylocale) : strcoll(a1p, a2p);
#else
        result = strcoll(a1p, a2p);
#endif
        /*
         * In some locales, strcoll() can claim that nonidentical strings are
         * equal.  Believing that would be bad news for a number of reasons,
         * so we follow Perl's lead and sort the "equal" strings according to
         * strcmp().
         */
        if (result == 0) {
            result = strcmp(a1p, a2p);
        }

        if (a1p != a1buf) {
            DstorePfreeExt(a1p);
        }
        if (a2p != a2buf) {
            DstorePfreeExt(a2p);
        }
    }

    return result;
}

/* TextCmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
int TextCmp(text* arg1, text* arg2, Oid collid)
{
    char *a1p = nullptr, *a2p = nullptr;
    int len1, len2;

    a1p = VarDataAny(arg1);
    a2p = VarDataAny(arg2);

    len1 = static_cast<int>(DSTORE_VARSIZE_ANY_EXHDR(arg1));
    len2 = static_cast<int>(DSTORE_VARSIZE_ANY_EXHDR(arg2));
    if (len1 < 0 && len2 >= 0) {
        return -1;
    }
    if (len1 < 0 && len2 < 0) {
        return 0;
    }
    if (len1 >= 0 && len2 < 0) {
        return 1;
    }

    int ret = VarstrCmp(a1p, len1, a2p, len2, collid);
    return ret > 0 ? 1 : ret == 0 ? 0 : -1;
}

}
