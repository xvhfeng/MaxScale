/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** An tokenizer for SQL
**
** This file contains C code that splits an SQL input string up into
** individual tokens and sends those tokens one-by-one over to the
** parser for analysis.
*/
#include "sqliteInt.h"
#include <stdlib.h>

/* Character classes for tokenizing
**
** In the sqlite3GetToken() function, a switch() on aiClass[c] is implemented
** using a lookup table, whereas a switch() directly on c uses a binary search.
** The lookup table is much faster.  To maximize speed, and to ensure that
** a lookup table is used, all of the classes need to be small integers and
** all of them need to be used within the switch.
*/
#define CC_X          0    /* The letter 'x', or start of BLOB literal */
#define CC_KYWD       1    /* Alphabetics or '_'.  Usable in a keyword */
#define CC_ID         2    /* unicode characters usable in IDs */
#define CC_DIGIT      3    /* Digits */
#define CC_DOLLAR     4    /* '$' */
#define CC_VARALPHA   5    /* '@', '#', ':'.  Alphabetic SQL variables */
#define CC_VARNUM     6    /* '?'.  Numeric SQL variables */
#define CC_SPACE      7    /* Space characters */
#define CC_QUOTE      8    /* '"', '\'', or '`'.  String literals, quoted ids */
#define CC_QUOTE2     9    /* '['.   [...] style quoted ids */
#define CC_PIPE      10    /* '|'.   Bitwise OR or concatenate */
#define CC_MINUS     11    /* '-'.  Minus or SQL-style comment */
#define CC_LT        12    /* '<'.  Part of < or <= or <> */
#define CC_GT        13    /* '>'.  Part of > or >= */
#define CC_EQ        14    /* '='.  Part of = or == */
#define CC_BANG      15    /* '!'.  Part of != */
#define CC_SLASH     16    /* '/'.  / or c-style comment */
#define CC_LP        17    /* '(' */
#define CC_RP        18    /* ')' */
#define CC_SEMI      19    /* ';' */
#define CC_PLUS      20    /* '+' */
#define CC_STAR      21    /* '*' */
#define CC_PERCENT   22    /* '%' */
#define CC_COMMA     23    /* ',' */
#define CC_AND       24    /* '&' */
#define CC_TILDA     25    /* '~' */
#define CC_DOT       26    /* '.' */
#define CC_ILLEGAL   27    /* Illegal character */

static const unsigned char aiClass[] = {
#ifdef SQLITE_ASCII
/*         x0  x1  x2  x3  x4  x5  x6  x7  x8  x9  xa  xb  xc  xd  xe  xf */
/* 0x */   27, 27, 27, 27, 27, 27, 27, 27, 27,  7,  7, 27,  7,  7, 27, 27,
/* 1x */   27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
#ifdef MAXSCALE
/* 2x */    7, 15,  8,  5,  2, 22, 24,  8, 17, 18, 21, 20, 23, 11, 26, 16,
#else
/* 2x */    7, 15,  8,  5,  4, 22, 24,  8, 17, 18, 21, 20, 23, 11, 26, 16,
#endif
/* 3x */    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  5, 19, 12, 14, 13,  6,
/* 4x */    5,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
/* 5x */    1,  1,  1,  1,  1,  1,  1,  1,  0,  1,  1,  9, 27, 27, 27,  1,
/* 6x */    8,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
/* 7x */    1,  1,  1,  1,  1,  1,  1,  1,  0,  1,  1, 27, 10, 27, 25, 27,
/* 8x */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* 9x */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Ax */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Bx */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Cx */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Dx */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Ex */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
/* Fx */    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2
#endif
#ifdef SQLITE_EBCDIC
/*         x0  x1  x2  x3  x4  x5  x6  x7  x8  x9  xa  xb  xc  xd  xe  xf */
/* 0x */   27, 27, 27, 27, 27,  7, 27, 27, 27, 27, 27, 27,  7,  7, 27, 27,
/* 1x */   27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
/* 2x */   27, 27, 27, 27, 27,  7, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
/* 3x */   27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
/* 4x */    7, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 12, 17, 20, 10,
/* 5x */   24, 27, 27, 27, 27, 27, 27, 27, 27, 27, 15,  4, 21, 18, 19, 27,
/* 6x */   11, 16, 27, 27, 27, 27, 27, 27, 27, 27, 27, 23, 22,  1, 13,  7,
/* 7x */   27, 27, 27, 27, 27, 27, 27, 27, 27,  8,  5,  5,  5,  8, 14,  8,
/* 8x */   27,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
/* 9x */   27,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
/* 9x */   25,  1,  1,  1,  1,  1,  1,  0,  1,  1, 27, 27, 27, 27, 27, 27,
/* Bx */   27, 27, 27, 27, 27, 27, 27, 27, 27, 27,  9, 27, 27, 27, 27, 27,
/* Cx */   27,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
/* Dx */   27,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
/* Ex */   27, 27,  1,  1,  1,  1,  1,  0,  1,  1, 27, 27, 27, 27, 27, 27,
/* Fx */    3,  3,  3,  3,  3,  3,  3,  3,  3,  3, 27, 27, 27, 27, 27, 27,
#endif
};

/*
** The charMap() macro maps alphabetic characters (only) into their
** lower-case ASCII equivalent.  On ASCII machines, this is just
** an upper-to-lower case map.  On EBCDIC machines we also need
** to adjust the encoding.  The mapping is only valid for alphabetics
** which are the only characters for which this feature is used. 
**
** Used by keywordhash.h
*/
#ifdef SQLITE_ASCII
# define charMap(X) sqlite3UpperToLower[(unsigned char)X]
#endif
#ifdef SQLITE_EBCDIC
# define charMap(X) ebcdicToAscii[(unsigned char)X]
const unsigned char ebcdicToAscii[] = {
/* 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 0x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 1x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 2x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 3x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 4x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 5x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 95,  0,  0,  /* 6x */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 7x */
   0, 97, 98, 99,100,101,102,103,104,105,  0,  0,  0,  0,  0,  0,  /* 8x */
   0,106,107,108,109,110,111,112,113,114,  0,  0,  0,  0,  0,  0,  /* 9x */
   0,  0,115,116,117,118,119,120,121,122,  0,  0,  0,  0,  0,  0,  /* Ax */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* Bx */
   0, 97, 98, 99,100,101,102,103,104,105,  0,  0,  0,  0,  0,  0,  /* Cx */
   0,106,107,108,109,110,111,112,113,114,  0,  0,  0,  0,  0,  0,  /* Dx */
   0,  0,115,116,117,118,119,120,121,122,  0,  0,  0,  0,  0,  0,  /* Ex */
   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* Fx */
};
#endif

/*
** The sqlite3KeywordCode function looks up an identifier to determine if
** it is a keyword.  If it is a keyword, the token code of that keyword is 
** returned.  If the input is not a keyword, TK_ID is returned.
**
** The implementation of this routine was generated by a program,
** mkkeywordhash.c, located in the tool subdirectory of the distribution.
** The output of the mkkeywordhash.c program is written into a file
** named keywordhash.h and then included into this source file by
** the #include below.
*/
#include "keywordhash.h"


/*
** If X is a character that can be used in an identifier then
** IdChar(X) will be true.  Otherwise it is false.
**
** For ASCII, any character with the high-order bit set is
** allowed in an identifier.  For 7-bit characters, 
** sqlite3IsIdChar[X] must be 1.
**
** For EBCDIC, the rules are more complex but have the same
** end result.
**
** Ticket #1066.  the SQL standard does not allow '$' in the
** middle of identifiers.  But many SQL implementations do. 
** SQLite will allow '$' in identifiers for compatibility.
** But the feature is undocumented.
*/
#ifdef SQLITE_ASCII
#define IdChar(C)  ((sqlite3CtypeMap[(unsigned char)C]&0x46)!=0)
#endif
#ifdef SQLITE_EBCDIC
const char sqlite3IsEbcdicIdChar[] = {
/* x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 xA xB xC xD xE xF */
    0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,  /* 4x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0,  /* 5x */
    0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0,  /* 6x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0,  /* 7x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0,  /* 8x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0,  /* 9x */
    1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0,  /* Ax */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  /* Bx */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1,  /* Cx */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1,  /* Dx */
    0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1,  /* Ex */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0,  /* Fx */
};
#define IdChar(C)  (((c=C)>=0x42 && sqlite3IsEbcdicIdChar[c-0x40]))
#endif

/* Make the IdChar function accessible from ctime.c */
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
int sqlite3IsIdChar(u8 c){ return IdChar(c); }
#endif


/*
** Return the length (in bytes) of the token that begins at z[0]. 
** Store the token type in *tokenType before returning.
*/
#ifdef MAXSCALE
extern int maxscaleComment();

struct mxs_charset_entry
{
    const char* name;
    size_t      len;
};

// Character set names of MariaDB.
//
// NOTE: MUST be kept in alphabetical order.
const struct mxs_charset_entry mxs_charset_names[] =
{
    { "armscii8", 8 },
    { "ascii",    5 },
    { "big5",     4 },
    { "binary",   6 },
    { "cp1250",   6 },
    { "cp1251",   6 },
    { "cp1256",   6 },
    { "cp1257",   6 },
    { "cp850",    5 },
    { "cp852",    5 },
    { "cp866",    5 },
    { "cp932",    5 },
    { "dec8",     4 },
    { "eucjpms",  7 },
    { "euckr",    5 },
    { "gb2312",   6 },
    { "gbk",      3 },
    { "geostd8",  7 },
    { "greek",    5 },
    { "hebrew",   6 },
    { "hp8",      3 },
    { "keybcs2",  7 },
    { "koi8r",    5 },
    { "koi8u",    5 },
    { "latin1",   6 },
    { "latin2",   6 },
    { "latin5",   6 },
    { "latin7",   6 },
    { "macce",    5 },
    { "macroman", 8 },
    { "sjis",     4 },
    { "swe7",     4 },
    { "tis620",   6 },
    { "ucs2",     4 },
    { "ujis",     4 },
    { "utf16",    5 },
    { "utf16le",  7 },
    { "utf32",    5 },
    { "utf8",     4 },
    { "utf8mb4",  7 }
};

#define N_MXS_CHARSET_NAMES (sizeof(mxs_charset_names)/sizeof(mxs_charset_names[0]))

int mxs_compare_charset_names(const void* l, const void* r)
{
    const struct mxs_charset_entry* key = (const struct mxs_charset_entry*)l;
    const struct mxs_charset_entry* value = (const struct mxs_charset_entry*)r;

    int rv = strncasecmp(key->name, value->name, MIN(key->len, value->len));

    if (key->len != value->len)
    {
        if (rv == 0)
        {
            rv = key->len < value->len ? -1 : 1;
        }
    }

    return rv;
}

int mxs_is_charset_name(const char* p, size_t n)
{
    struct mxs_charset_entry key = { p, n };

    return bsearch(&key,
                   mxs_charset_names, N_MXS_CHARSET_NAMES, sizeof(mxs_charset_names[0]),
                   mxs_compare_charset_names) != 0;
}



int sqlite3GetToken(Parse* pParse, const unsigned char *z, int *tokenType){
#else
int sqlite3GetToken(const unsigned char *z, int *tokenType){
#endif
  int i, c;
  switch( aiClass[*z] ){  /* Switch on the character-class of the first byte
                          ** of the token. See the comment on the CC_ defines
                          ** above. */
    case CC_SPACE: {
      testcase( z[0]==' ' );
      testcase( z[0]=='\t' );
      testcase( z[0]=='\n' );
      testcase( z[0]=='\f' );
      testcase( z[0]=='\r' );
      for(i=1; sqlite3Isspace(z[i]); i++){}
      *tokenType = TK_SPACE;
      return i;
    }
    case CC_MINUS: {
      if( z[1]=='-' ){
#ifdef MAXSCALE
        maxscaleComment();
#endif
        for(i=2; (c=z[i])!=0 && c!='\n'; i++){}
        *tokenType = TK_SPACE;   /* IMP: R-22934-25134 */
        return i;
      }
      *tokenType = TK_MINUS;
      return 1;
    }
    case CC_LP: {
      *tokenType = TK_LP;
      return 1;
    }
    case CC_RP: {
      *tokenType = TK_RP;
      return 1;
    }
    case CC_SEMI: {
      *tokenType = TK_SEMI;
      return 1;
    }
    case CC_PLUS: {
      *tokenType = TK_PLUS;
      return 1;
    }
    case CC_STAR: {
      *tokenType = TK_STAR;
      return 1;
    }
    case CC_SLASH: {
      if( z[1]!='*' || z[2]==0 ){
        *tokenType = TK_SLASH;
        return 1;
      }
#ifdef MAXSCALE
      if ( z[2]=='!' || (z[2]=='M' && z[3]=='!')){
        int j = (z[2]=='M' ? 4 : 3);
        // MySQL-specific code
        for (i=j, c=z[j-1]; (c!='*' || z[i]!='/') && (c=z[i])!=0; i++){}
        if (c=='*' && z[i]=='/'){
          if (sqlite3Isdigit(z[j])) {
            // A version specific executable comment,
            // e.g. "/*!99999 ..." or "/*M!99999 ..." => never parsed.
            extern void maxscaleSetStatusCap(int);
            maxscaleSetStatusCap(2); // QC_QUERY_PARTIALLY_PARSED, see parser.hh:Parser::Result
            ++i; // Next after the trailing '/'
          }
          else {
            // A non-version specific executable comment,
            // e.g."/*! select 1 */ or "/*M! select 1 */ => always parsed.
            char* znc = (char*) z;
            znc[0]=znc[1]=znc[2]=znc[i-1]=znc[i]=' '; // Remove comment chars, i.e. "/*!" and "*/".
            if (j==4){
              znc[3]=0; // It wasn't "/*!" but "/*M!".
            }
            for (i=j; sqlite3Isspace(z[i]); ++i){} // Jump over any space.
          }
        }
      } else {
        for(i=3, c=z[2]; (c!='*' || z[i]!='/') && (c=z[i])!=0; i++){}

        if( c ) i++;
      }
#else
      for(i=3, c=z[2]; (c!='*' || z[i]!='/') && (c=z[i])!=0; i++){}
      if( c ) i++;
#endif
      *tokenType = TK_SPACE;   /* IMP: R-22934-25134 */
      return i;
    }
    case CC_PERCENT: {
      *tokenType = TK_REM;
      return 1;
    }
    case CC_EQ: {
      *tokenType = TK_EQ;
      return 1 + (z[1]=='=');
    }
    case CC_LT: {
      if( (c=z[1])=='=' ){
#ifdef MAXSCALE
        if ( z[2]=='>' ){
          // "<=>"
          *tokenType = TK_EQ;
          return 3;
        }
#endif
        *tokenType = TK_LE;
        return 2;
      }else if( c=='>' ){
        *tokenType = TK_NE;
        return 2;
      }else if( c=='<' ){
        *tokenType = TK_LSHIFT;
        return 2;
      }else{
        *tokenType = TK_LT;
        return 1;
      }
    }
    case CC_GT: {
      if( (c=z[1])=='=' ){
        *tokenType = TK_GE;
        return 2;
      }else if( c=='>' ){
        *tokenType = TK_RSHIFT;
        return 2;
      }else{
        *tokenType = TK_GT;
        return 1;
      }
    }
    case CC_BANG: {
      if( z[1]!='=' ){
#ifdef MAXSCALE
        *tokenType = TK_NOT;
        return 1;
#else
        *tokenType = TK_ILLEGAL;
        return 2;
#endif
      }else{
        *tokenType = TK_NE;
        return 2;
      }
    }
    case CC_PIPE: {
      if( z[1]!='|' ){
        *tokenType = TK_BITOR;
        return 1;
      }else{
        *tokenType = TK_CONCAT;
        return 2;
      }
    }
    case CC_COMMA: {
      *tokenType = TK_COMMA;
      return 1;
    }
    case CC_AND: {
      *tokenType = TK_BITAND;
      return 1;
    }
    case CC_TILDA: {
      *tokenType = TK_BITNOT;
      return 1;
    }
    case CC_QUOTE: {
      int delim = z[0];
      testcase( delim=='`' );
      testcase( delim=='\'' );
      testcase( delim=='"' );
      for(i=1; (c=z[i])!=0; i++){
        if( c==delim ){
          if( z[i+1]==delim ){
            i++;
          }else{
            break;
          }
#ifdef MAXSCALE
        }else if (c == '\\' ){
          if ( z[i+1] ){
            i++;
          }
#endif
        }
      }
#ifdef MAXSCALE
      if( c=='\'' || c=='"' ){
#else
      if( c=='\'' ){
#endif
        *tokenType = TK_STRING;
        return i+1;
      }else if( c!=0 ){
        *tokenType = TK_ID;
        return i+1;
      }else{
        *tokenType = TK_ILLEGAL;
        return i;
      }
    }
    case CC_DOT: {
#ifndef SQLITE_OMIT_FLOATING_POINT
      if( !sqlite3Isdigit(z[1]) )
#endif
      {
        *tokenType = TK_DOT;
        return 1;
      }
#ifdef MAXSCALE
      /* Next char is a digit, we need to sniff further to find out whether it
      ** is an identifer that starts with a digit.
      */
      int j=1;
      int nondigitChars=0;
      while ( IdChar(z[j]) ){
        if ( !sqlite3Isdigit(z[j++]) ){
          ++nondigitChars;
        }
      }
      if ( nondigitChars ){
        // At least one char that is not a digit => an id (and not a float) coming.
        *tokenType = TK_DOT;
        return 1;
      }
#endif
      /* If the next character is a digit, this is a floating point
      ** number that begins with ".".  Fall thru into the next case */
    }
    case CC_DIGIT: {
      testcase( z[0]=='0' );  testcase( z[0]=='1' );  testcase( z[0]=='2' );
      testcase( z[0]=='3' );  testcase( z[0]=='4' );  testcase( z[0]=='5' );
      testcase( z[0]=='6' );  testcase( z[0]=='7' );  testcase( z[0]=='8' );
      testcase( z[0]=='9' );
      *tokenType = TK_INTEGER;
#ifndef SQLITE_OMIT_HEX_INTEGER
      if( z[0]=='0' && (z[1]=='x' || z[1]=='X') && sqlite3Isxdigit(z[2]) ){
        for(i=3; sqlite3Isxdigit(z[i]); i++){}
        return i;
      }
#endif
      for(i=0; sqlite3Isdigit(z[i]); i++){}
#ifndef SQLITE_OMIT_FLOATING_POINT
      if( z[i]=='.' ){
        i++;
        while( sqlite3Isdigit(z[i]) ){ i++; }
        *tokenType = TK_FLOAT;
      }
      if( (z[i]=='e' || z[i]=='E') &&
           ( sqlite3Isdigit(z[i+1]) 
            || ((z[i+1]=='+' || z[i+1]=='-') && sqlite3Isdigit(z[i+2]))
           )
      ){
        i += 2;
        while( sqlite3Isdigit(z[i]) ){ i++; }
        *tokenType = TK_FLOAT;
      }
#endif
      while( IdChar(z[i]) ){
#ifdef MAXSCALE
        *tokenType = TK_ID;
#else
        *tokenType = TK_ILLEGAL;
#endif
        i++;
      }
      return i;
    }
    case CC_QUOTE2: {
      for(i=1, c=z[0]; c!=']' && (c=z[i])!=0; i++){}
      *tokenType = c==']' ? TK_ID : TK_ILLEGAL;
      return i;
    }
    case CC_VARNUM: {
      *tokenType = TK_VARIABLE;
      for(i=1; sqlite3Isdigit(z[i]); i++){}
      return i;
    }
    case CC_DOLLAR:
    case CC_VARALPHA: {
      int n = 0;
      testcase( z[0]=='$' );  testcase( z[0]=='@' );
      testcase( z[0]==':' );  testcase( z[0]=='#' );
#ifdef MAXSCALE
      if (z[0]=='#') {
        if (maxscaleComment()) {
          for(i=1; (c=z[i])!=0 && c!='\n'; i++){}
          *tokenType = TK_SPACE;
          return i;
        }
      }
      if (z[0]==':' && z[1]=='=') {
        *tokenType = TK_EQ;
        return 2;
      }
#endif
      *tokenType = TK_VARIABLE;
      for(i=1; (c=z[i])!=0; i++){
#ifdef MAXSCALE
          if ( (i == 1) && (z[0] == '@') && (c == '@') ) {
          // If the first char is '@' then if the second char is a '@'
          // it is a system variable (@@xyz).
          continue;
        }else if( IdChar(c) ){
#else
        if( IdChar(c) ){
#endif
          n++;
#ifndef SQLITE_OMIT_TCL_VARIABLE
        }else if( c=='(' && n>0 ){
          do{
            i++;
          }while( (c=z[i])!=0 && !sqlite3Isspace(c) && c!=')' );
          if( c==')' ){
            i++;
          }else{
            *tokenType = TK_ILLEGAL;
          }
          break;
        }else if( c==':' && z[i+1]==':' ){
          i++;
#endif
#ifdef MAXSCALE
        }else if ( c=='\'' || c=='"' || c=='`' ){
          int q=c;
          ++i;
          while ( IdChar(z[i]) ) {
            ++i;
            ++n;
          }
          if ( z[i]==q )
          {
            ++i;
            break;
          }
#endif
        }else{
          break;
        }
      }
      if( n==0 ) *tokenType = TK_ILLEGAL;
      return i;
    }
    case CC_KYWD: {
#ifdef MAXSCALE
      // This is for bit fields, e.g. b'10110111'.
      if ( z[0]=='b' || z[0]=='B' ) {
        if ( z[1]=='\'' ){
          // We return it as an integer. We are not interested in the value
          *tokenType = TK_INTEGER;
          for(i=2; (z[i]=='0'||z[i]=='1'); i++){}
          if ( z[i]!='\'' ){
            *tokenType = TK_ILLEGAL;
            while ( z[i] && z[i]!='\'' ){ i++; }
          }
          if ( z[i] ) i++;
          return i;
        }
      }
      /* Not a bit field. It may be a keyword so we flow through */
#endif
      for(i=1; aiClass[z[i]]<=CC_KYWD; i++){}
#ifdef MAXSCALE
      if ( z[0]== '_' ) {
        // This can be a case of [_charset_name], so we need to
        // accept more. We can eat all characters acceptable for
        // an identifier.
        while ( IdChar(z[i]) ) { i++; }

        if (mxs_is_charset_name((char*)z + 1, i - 1)) {
            *tokenType = TK_CHARSET_NAME_KW;
            return i;
        } else {
            // Token type will be TK_ID.
            break;
        }
      }
#endif
      if( IdChar(z[i]) ){
        /* This token started out using characters that can appear in keywords,
        ** but z[i] is a character not allowed within keywords, so this must
        ** be an identifier instead */
        i++;
        break;
      }
      *tokenType = TK_ID;
#ifdef MAXSCALE
      i = keywordCode((char*)z, i, tokenType);
      if (*tokenType != TK_ID)
      {
        if (pParse) {
          if (z != (const unsigned char *)pParse->zTail) {
            const char *p = (const char*)z - 1;
            while ((p != pParse->zTail) && sqlite3Isspace(*p) && *p != '\n') {
              --p;
            }

            if (*p == '.') {
              /* If the last character before the keyword is '.' then
              ** we assume this token is the second part of a qualified
              ** name, e.g. "tbl1.index" in which case we return the
              ** keyword as an id.
              */
              *tokenType = TK_ID;
            }
          }
        }

        if (*tokenType != TK_ID) {
          extern int maxscaleKeyword(int);
          extern int maxscaleTranslateKeyword(int);

          *tokenType = maxscaleTranslateKeyword(*tokenType);

          if (*tokenType != TK_ID) {
            if (maxscaleKeyword(*tokenType) != 0)
            {
              /* Consume the entire string. */
              while ( z[i] ) {
                ++i;
              }
            }
          }
        }
      }
      return i;
#else
      return keywordCode((char*)z, i, tokenType);
#endif
    }
#ifndef SQLITE_OMIT_BLOB_LITERAL
    case CC_X: {
      testcase( z[0]=='x' ); testcase( z[0]=='X' );
      if( z[1]=='\'' ){
        *tokenType = TK_BLOB;
        for(i=2; sqlite3Isxdigit(z[i]); i++){}
        if( z[i]!='\'' || i%2 ){
          *tokenType = TK_ILLEGAL;
          while( z[i] && z[i]!='\'' ){ i++; }
        }
        if( z[i] ) i++;
        return i;
      }
      /* If it is not a BLOB literal, then it must be an ID, since no
      ** SQL keywords start with the letter 'x'.  Fall through */
    }
#endif
#ifdef MAXSCALE
    // It may be the "XA" keyword.
    // If the next character is 'a' or 'A', followed by whitespace or a
    // comment, then we are indeed dealing with the "XA" keyword.
    if (( z[1]=='a' || z[1]=='A' ) &&
        (sqlite3Isspace(z[2]) ||                              // Whitespace
         (z[2]=='/' && z[3]=='*') ||                          // Beginning of /* comment
         (z[2]=='#') ||                                       // # eol comment
         (z[2]=='-' && z[3]=='-' && sqlite3Isspace(z[4])))) { // --  eol comment
      extern int maxscaleKeyword(int);

      *tokenType = TK_XA;
      maxscaleKeyword(*tokenType);
      return 2;
    }
#endif
    case CC_ID: {
      i = 1;
      break;
    }
    default: {
      *tokenType = TK_ILLEGAL;
      return 1;
    }
  }
  while( IdChar(z[i]) ){ i++; }
  *tokenType = TK_ID;
  return i;
}

/*
** Run the parser on the given SQL string.  The parser structure is
** passed in.  An SQLITE_ status code is returned.  If an error occurs
** then an and attempt is made to write an error message into 
** memory obtained from sqlite3_malloc() and to make *pzErrMsg point to that
** error message.
*/
int sqlite3RunParser(Parse *pParse, const char *zSql, char **pzErrMsg){
  int nErr = 0;                   /* Number of errors encountered */
  int i;                          /* Loop counter */
  void *pEngine;                  /* The LEMON-generated LALR(1) parser */
  int tokenType;                  /* type of the next token */
  int lastTokenParsed = -1;       /* type of the previous token */
  sqlite3 *db = pParse->db;       /* The database connection */
  int mxSqlLen;                   /* Max length of an SQL string */

  assert( zSql!=0 );
  mxSqlLen = db->aLimit[SQLITE_LIMIT_SQL_LENGTH];
  if( db->nVdbeActive==0 ){
    db->u1.isInterrupted = 0;
  }
  pParse->rc = SQLITE_OK;
  pParse->zTail = zSql;
  i = 0;
  assert( pzErrMsg!=0 );
  /* sqlite3ParserTrace(stdout, "parser: "); */
  pEngine = sqlite3ParserAlloc(sqlite3Malloc);
  if( pEngine==0 ){
    sqlite3OomFault(db);
    return SQLITE_NOMEM;
  }
  assert( pParse->pNewTable==0 );
  assert( pParse->pNewTrigger==0 );
  assert( pParse->nVar==0 );
  assert( pParse->nzVar==0 );
  assert( pParse->azVar==0 );
  while( zSql[i]!=0 ){
    assert( i>=0 );
    pParse->sLastToken.z = &zSql[i];
#ifdef MAXSCALE
    pParse->sLastToken.n = sqlite3GetToken(pParse,(unsigned char*)&zSql[i],&tokenType);
#else
    pParse->sLastToken.n = sqlite3GetToken((unsigned char*)&zSql[i],&tokenType);
#endif
    i += pParse->sLastToken.n;
    if( i>mxSqlLen ){
      pParse->rc = SQLITE_TOOBIG;
      break;
    }
    if( tokenType>=TK_SPACE ){
      assert( tokenType==TK_SPACE || tokenType==TK_ILLEGAL );
      if( db->u1.isInterrupted ){
        pParse->rc = SQLITE_INTERRUPT;
        break;
      }
      if( tokenType==TK_ILLEGAL ){
        sqlite3ErrorMsg(pParse, "unrecognized token: \"%T\"",
                        &pParse->sLastToken);
        break;
      }
    }else{
      if( tokenType==TK_SEMI ) pParse->zTail = &zSql[i];
      sqlite3Parser(pEngine, tokenType, pParse->sLastToken, pParse);
      lastTokenParsed = tokenType;
      if( pParse->rc!=SQLITE_OK || db->mallocFailed ) break;
    }
  }
  assert( nErr==0 );
  if( pParse->rc==SQLITE_OK && db->mallocFailed==0 ){
    assert( zSql[i]==0 );
    if( lastTokenParsed!=TK_SEMI ){
      sqlite3Parser(pEngine, TK_SEMI, pParse->sLastToken, pParse);
      pParse->zTail = &zSql[i];
    }
    if( pParse->rc==SQLITE_OK && db->mallocFailed==0 ){
      sqlite3Parser(pEngine, 0, pParse->sLastToken, pParse);
    }
  }
#ifdef YYTRACKMAXSTACKDEPTH
  sqlite3_mutex_enter(sqlite3MallocMutex());
  sqlite3StatusHighwater(SQLITE_STATUS_PARSER_STACK,
      sqlite3ParserStackPeak(pEngine)
  );
  sqlite3_mutex_leave(sqlite3MallocMutex());
#endif /* YYDEBUG */
  sqlite3ParserFree(pEngine, sqlite3_free);
  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM;
  }
  if( pParse->rc!=SQLITE_OK && pParse->rc!=SQLITE_DONE && pParse->zErrMsg==0 ){
    pParse->zErrMsg = sqlite3MPrintf(db, "%s", sqlite3ErrStr(pParse->rc));
  }
  assert( pzErrMsg!=0 );
  if( pParse->zErrMsg ){
    *pzErrMsg = pParse->zErrMsg;
    sqlite3_log(pParse->rc, "%s", *pzErrMsg);
    pParse->zErrMsg = 0;
    nErr++;
  }
  if( pParse->pVdbe && pParse->nErr>0 && pParse->nested==0 ){
    sqlite3VdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
  }
#ifndef SQLITE_OMIT_SHARED_CACHE
  if( pParse->nested==0 ){
    sqlite3DbFree(db, pParse->aTableLock);
    pParse->aTableLock = 0;
    pParse->nTableLock = 0;
  }
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite3_free(pParse->apVtabLock);
#endif

  if( !IN_DECLARE_VTAB ){
    /* If the pParse->declareVtab flag is set, do not delete any table 
    ** structure built up in pParse->pNewTable. The calling code (see vtab.c)
    ** will take responsibility for freeing the Table structure.
    */
    sqlite3DeleteTable(db, pParse->pNewTable);
  }

  sqlite3WithDelete(db, pParse->pWithToFree);
  sqlite3DeleteTrigger(db, pParse->pNewTrigger);
  for(i=pParse->nzVar-1; i>=0; i--) sqlite3DbFree(db, pParse->azVar[i]);
  sqlite3DbFree(db, pParse->azVar);
  while( pParse->pAinc ){
    AutoincInfo *p = pParse->pAinc;
    pParse->pAinc = p->pNext;
    sqlite3DbFree(db, p);
  }
  while( pParse->pZombieTab ){
    Table *p = pParse->pZombieTab;
    pParse->pZombieTab = p->pNextZombie;
    sqlite3DeleteTable(db, p);
  }
  assert( nErr==0 || pParse->rc!=SQLITE_OK );
  return nErr;
}
