package org.apache.spark.sql.catalyst.parser;// Generated from /Users/noone/IdeaProjects/spark/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 by ANTLR 4.7.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, ADD=11, AFTER=12, ALL=13, ALTER=14, ANALYZE=15, AND=16, ANTI=17, 
		ANY=18, ARCHIVE=19, ARRAY=20, AS=21, ASC=22, AT=23, AUTHORIZATION=24, 
		BETWEEN=25, BOTH=26, BUCKET=27, BUCKETS=28, BY=29, CACHE=30, CASCADE=31, 
		CASE=32, CAST=33, CHANGE=34, CHECK=35, CLEAR=36, CLUSTER=37, CLUSTERED=38, 
		CODEGEN=39, COLLATE=40, COLLECTION=41, COLUMN=42, COLUMNS=43, COMMENT=44, 
		COMMIT=45, COMPACT=46, COMPACTIONS=47, COMPUTE=48, CONCATENATE=49, CONSTRAINT=50, 
		COST=51, CREATE=52, CROSS=53, CUBE=54, CURRENT=55, CURRENT_DATE=56, CURRENT_TIME=57, 
		CURRENT_TIMESTAMP=58, CURRENT_USER=59, DATA=60, DATABASE=61, DATABASES=62, 
		DAY=63, DAYS=64, DBPROPERTIES=65, DEFINED=66, DELETE=67, DELIMITED=68, 
		DESC=69, DESCRIBE=70, DFS=71, DIRECTORIES=72, DIRECTORY=73, DISTINCT=74, 
		DISTRIBUTE=75, DROP=76, ELSE=77, END=78, ESCAPED=79, EXCEPT=80, EXCHANGE=81, 
		EXISTS=82, EXPLAIN=83, EXPORT=84, EXTENDED=85, EXTERNAL=86, EXTRACT=87, 
		FALSE=88, FETCH=89, FIELDS=90, FILEFORMAT=91, FIRST=92, FOLLOWING=93, 
		FOR=94, FOREIGN=95, FORMAT=96, FORMATTED=97, FROM=98, FULL=99, FUNCTION=100, 
		FUNCTIONS=101, GLOBAL=102, GRANT=103, GROUP=104, GROUPING=105, HAVING=106, 
		HOUR=107, HOURS=108, IF=109, IGNORE=110, IMPORT=111, IN=112, INDEX=113, 
		INDEXES=114, INNER=115, INPATH=116, INPUTFORMAT=117, INSERT=118, INTERSECT=119, 
		INTERVAL=120, INTO=121, IS=122, ITEMS=123, JOIN=124, KEYS=125, LAST=126, 
		LATERAL=127, LAZY=128, LEADING=129, LEFT=130, LIKE=131, LIMIT=132, LINES=133, 
		LIST=134, LOAD=135, LOCAL=136, LOCATION=137, LOCK=138, LOCKS=139, LOGICAL=140, 
		MACRO=141, MAP=142, MICROSECOND=143, MICROSECONDS=144, MILLISECOND=145, 
		MILLISECONDS=146, MINUTE=147, MINUTES=148, MONTH=149, MONTHS=150, MSCK=151, 
		NATURAL=152, NO=153, NOT=154, NULL=155, NULLS=156, OF=157, ON=158, ONLY=159, 
		OPTION=160, OPTIONS=161, OR=162, ORDER=163, OUT=164, OUTER=165, OUTPUTFORMAT=166, 
		OVER=167, OVERLAPS=168, OVERWRITE=169, PARTITION=170, PARTITIONED=171, 
		PARTITIONS=172, PERCENTLIT=173, PIVOT=174, POSITION=175, PRECEDING=176, 
		PRIMARY=177, PRINCIPALS=178, PURGE=179, QUERY=180, RANGE=181, RECORDREADER=182, 
		RECORDWRITER=183, RECOVER=184, REDUCE=185, REFERENCES=186, REFRESH=187, 
		RENAME=188, REPAIR=189, REPLACE=190, RESET=191, RESTRICT=192, REVOKE=193, 
		RIGHT=194, RLIKE=195, ROLE=196, ROLES=197, ROLLBACK=198, ROLLUP=199, ROW=200, 
		ROWS=201, SCHEMA=202, SECOND=203, SECONDS=204, SELECT=205, SEMI=206, SEPARATED=207, 
		SERDE=208, SERDEPROPERTIES=209, SESSION_USER=210, SET=211, SETMINUS=212, 
		SETS=213, SHOW=214, SKEWED=215, SOME=216, SORT=217, SORTED=218, START=219, 
		STATISTICS=220, STORED=221, STRATIFY=222, STRUCT=223, TABLE=224, TABLES=225, 
		TABLESAMPLE=226, TBLPROPERTIES=227, TEMPORARY=228, TERMINATED=229, THEN=230, 
		TO=231, TOUCH=232, TRAILING=233, TRANSACTION=234, TRANSACTIONS=235, TRANSFORM=236, 
		TRUE=237, TRUNCATE=238, UNARCHIVE=239, UNBOUNDED=240, UNCACHE=241, UNION=242, 
		UNIQUE=243, UNLOCK=244, UNSET=245, USE=246, USER=247, USING=248, VALUES=249, 
		VIEW=250, WEEK=251, WEEKS=252, WHEN=253, WHERE=254, WINDOW=255, WITH=256, 
		YEAR=257, YEARS=258, EQ=259, NSEQ=260, NEQ=261, NEQJ=262, LT=263, LTE=264, 
		GT=265, GTE=266, PLUS=267, MINUS=268, ASTERISK=269, SLASH=270, PERCENT=271, 
		DIV=272, TILDE=273, AMPERSAND=274, PIPE=275, CONCAT_PIPE=276, HAT=277, 
		STRING=278, BIGINT_LITERAL=279, SMALLINT_LITERAL=280, TINYINT_LITERAL=281, 
		INTEGER_VALUE=282, DECIMAL_VALUE=283, DOUBLE_LITERAL=284, BIGDECIMAL_LITERAL=285, 
		IDENTIFIER=286, BACKQUOTED_IDENTIFIER=287, SIMPLE_COMMENT=288, BRACKETED_EMPTY_COMMENT=289, 
		BRACKETED_COMMENT=290, WS=291, UNRECOGNIZED=292;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleMultipartIdentifier = 3, RULE_singleFunctionIdentifier = 4, 
		RULE_singleDataType = 5, RULE_singleTableSchema = 6, RULE_statement = 7, 
		RULE_unsupportedHiveNativeCommands = 8, RULE_createTableHeader = 9, RULE_bucketSpec = 10, 
		RULE_skewSpec = 11, RULE_locationSpec = 12, RULE_query = 13, RULE_insertInto = 14, 
		RULE_partitionSpecLocation = 15, RULE_partitionSpec = 16, RULE_partitionVal = 17, 
		RULE_database = 18, RULE_describeFuncName = 19, RULE_describeColName = 20, 
		RULE_ctes = 21, RULE_namedQuery = 22, RULE_tableProvider = 23, RULE_tablePropertyList = 24, 
		RULE_tableProperty = 25, RULE_tablePropertyKey = 26, RULE_tablePropertyValue = 27, 
		RULE_constantList = 28, RULE_nestedConstantList = 29, RULE_createFileFormat = 30, 
		RULE_fileFormat = 31, RULE_storageHandler = 32, RULE_resource = 33, RULE_dmlStatementNoWith = 34, 
		RULE_queryNoWith = 35, RULE_queryOrganization = 36, RULE_multiInsertQueryBody = 37, 
		RULE_selectStatement = 38, RULE_queryTerm = 39, RULE_queryPrimary = 40, 
		RULE_sortItem = 41, RULE_querySpecification = 42, RULE_hint = 43, RULE_hintStatement = 44, 
		RULE_fromClause = 45, RULE_aggregation = 46, RULE_groupingSet = 47, RULE_pivotClause = 48, 
		RULE_pivotColumn = 49, RULE_pivotValue = 50, RULE_lateralView = 51, RULE_setQuantifier = 52, 
		RULE_relation = 53, RULE_joinRelation = 54, RULE_joinType = 55, RULE_joinCriteria = 56, 
		RULE_sample = 57, RULE_sampleMethod = 58, RULE_identifierList = 59, RULE_identifierSeq = 60, 
		RULE_orderedIdentifierList = 61, RULE_orderedIdentifier = 62, RULE_identifierCommentList = 63, 
		RULE_identifierComment = 64, RULE_relationPrimary = 65, RULE_inlineTable = 66, 
		RULE_functionTable = 67, RULE_tableAlias = 68, RULE_rowFormat = 69, RULE_multipartIdentifier = 70, 
		RULE_tableIdentifier = 71, RULE_functionIdentifier = 72, RULE_namedExpression = 73, 
		RULE_namedExpressionSeq = 74, RULE_transformList = 75, RULE_transform = 76, 
		RULE_transformArgument = 77, RULE_expression = 78, RULE_booleanExpression = 79, 
		RULE_predicate = 80, RULE_valueExpression = 81, RULE_primaryExpression = 82, 
		RULE_constant = 83, RULE_comparisonOperator = 84, RULE_arithmeticOperator = 85, 
		RULE_predicateOperator = 86, RULE_booleanValue = 87, RULE_interval = 88, 
		RULE_intervalField = 89, RULE_intervalValue = 90, RULE_intervalUnit = 91, 
		RULE_colPosition = 92, RULE_dataType = 93, RULE_colTypeList = 94, RULE_colType = 95, 
		RULE_complexColTypeList = 96, RULE_complexColType = 97, RULE_whenClause = 98, 
		RULE_windows = 99, RULE_namedWindow = 100, RULE_windowSpec = 101, RULE_windowFrame = 102, 
		RULE_frameBound = 103, RULE_qualifiedName = 104, RULE_identifier = 105, 
		RULE_strictIdentifier = 106, RULE_quotedIdentifier = 107, RULE_number = 108, 
		RULE_ansiNonReserved = 109, RULE_strictNonReserved = 110, RULE_nonReserved = 111;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "singleExpression", "singleTableIdentifier", "singleMultipartIdentifier", 
			"singleFunctionIdentifier", "singleDataType", "singleTableSchema", "statement", 
			"unsupportedHiveNativeCommands", "createTableHeader", "bucketSpec", "skewSpec", 
			"locationSpec", "query", "insertInto", "partitionSpecLocation", "partitionSpec", 
			"partitionVal", "database", "describeFuncName", "describeColName", "ctes", 
			"namedQuery", "tableProvider", "tablePropertyList", "tableProperty", 
			"tablePropertyKey", "tablePropertyValue", "constantList", "nestedConstantList", 
			"createFileFormat", "fileFormat", "storageHandler", "resource", "dmlStatementNoWith", 
			"queryNoWith", "queryOrganization", "multiInsertQueryBody", "selectStatement", 
			"queryTerm", "queryPrimary", "sortItem", "querySpecification", "hint", 
			"hintStatement", "fromClause", "aggregation", "groupingSet", "pivotClause", 
			"pivotColumn", "pivotValue", "lateralView", "setQuantifier", "relation", 
			"joinRelation", "joinType", "joinCriteria", "sample", "sampleMethod", 
			"identifierList", "identifierSeq", "orderedIdentifierList", "orderedIdentifier", 
			"identifierCommentList", "identifierComment", "relationPrimary", "inlineTable", 
			"functionTable", "tableAlias", "rowFormat", "multipartIdentifier", "tableIdentifier", 
			"functionIdentifier", "namedExpression", "namedExpressionSeq", "transformList", 
			"transform", "transformArgument", "expression", "booleanExpression", 
			"predicate", "valueExpression", "primaryExpression", "constant", "comparisonOperator", 
			"arithmeticOperator", "predicateOperator", "booleanValue", "interval", 
			"intervalField", "intervalValue", "intervalUnit", "colPosition", "dataType", 
			"colTypeList", "colType", "complexColTypeList", "complexColType", "whenClause", 
			"windows", "namedWindow", "windowSpec", "windowFrame", "frameBound", 
			"qualifiedName", "identifier", "strictIdentifier", "quotedIdentifier", 
			"number", "ansiNonReserved", "strictNonReserved", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "','", "'.'", "'/*+'", "'*/'", "'->'", "'['", "']'", 
			"':'", "'ADD'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", 
			"'ANTI'", "'ANY'", "'ARCHIVE'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", 
			"'BETWEEN'", "'BOTH'", "'BUCKET'", "'BUCKETS'", "'BY'", "'CACHE'", "'CASCADE'", 
			"'CASE'", "'CAST'", "'CHANGE'", "'CHECK'", "'CLEAR'", "'CLUSTER'", "'CLUSTERED'", 
			"'CODEGEN'", "'COLLATE'", "'COLLECTION'", "'COLUMN'", "'COLUMNS'", "'COMMENT'", 
			"'COMMIT'", "'COMPACT'", "'COMPACTIONS'", "'COMPUTE'", "'CONCATENATE'", 
			"'CONSTRAINT'", "'COST'", "'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", 
			"'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'CURRENT_USER'", 
			"'DATA'", "'DATABASE'", null, "'DAY'", "'DAYS'", "'DBPROPERTIES'", "'DEFINED'", 
			"'DELETE'", "'DELIMITED'", "'DESC'", "'DESCRIBE'", "'DFS'", "'DIRECTORIES'", 
			"'DIRECTORY'", "'DISTINCT'", "'DISTRIBUTE'", "'DROP'", "'ELSE'", "'END'", 
			"'ESCAPED'", "'EXCEPT'", "'EXCHANGE'", "'EXISTS'", "'EXPLAIN'", "'EXPORT'", 
			"'EXTENDED'", "'EXTERNAL'", "'EXTRACT'", "'FALSE'", "'FETCH'", "'FIELDS'", 
			"'FILEFORMAT'", "'FIRST'", "'FOLLOWING'", "'FOR'", "'FOREIGN'", "'FORMAT'", 
			"'FORMATTED'", "'FROM'", "'FULL'", "'FUNCTION'", "'FUNCTIONS'", "'GLOBAL'", 
			"'GRANT'", "'GROUP'", "'GROUPING'", "'HAVING'", "'HOUR'", "'HOURS'", 
			"'IF'", "'IGNORE'", "'IMPORT'", "'IN'", "'INDEX'", "'INDEXES'", "'INNER'", 
			"'INPATH'", "'INPUTFORMAT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", 
			"'INTO'", "'IS'", "'ITEMS'", "'JOIN'", "'KEYS'", "'LAST'", "'LATERAL'", 
			"'LAZY'", "'LEADING'", "'LEFT'", "'LIKE'", "'LIMIT'", "'LINES'", "'LIST'", 
			"'LOAD'", "'LOCAL'", "'LOCATION'", "'LOCK'", "'LOCKS'", "'LOGICAL'", 
			"'MACRO'", "'MAP'", "'MICROSECOND'", "'MICROSECONDS'", "'MILLISECOND'", 
			"'MILLISECONDS'", "'MINUTE'", "'MINUTES'", "'MONTH'", "'MONTHS'", "'MSCK'", 
			"'NATURAL'", "'NO'", null, "'NULL'", "'NULLS'", "'OF'", "'ON'", "'ONLY'", 
			"'OPTION'", "'OPTIONS'", "'OR'", "'ORDER'", "'OUT'", "'OUTER'", "'OUTPUTFORMAT'", 
			"'OVER'", "'OVERLAPS'", "'OVERWRITE'", "'PARTITION'", "'PARTITIONED'", 
			"'PARTITIONS'", "'PERCENT'", "'PIVOT'", "'POSITION'", "'PRECEDING'", 
			"'PRIMARY'", "'PRINCIPALS'", "'PURGE'", "'QUERY'", "'RANGE'", "'RECORDREADER'", 
			"'RECORDWRITER'", "'RECOVER'", "'REDUCE'", "'REFERENCES'", "'REFRESH'", 
			"'RENAME'", "'REPAIR'", "'REPLACE'", "'RESET'", "'RESTRICT'", "'REVOKE'", 
			"'RIGHT'", null, "'ROLE'", "'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", 
			"'ROWS'", "'SCHEMA'", "'SECOND'", "'SECONDS'", "'SELECT'", "'SEMI'", 
			"'SEPARATED'", "'SERDE'", "'SERDEPROPERTIES'", "'SESSION_USER'", "'SET'", 
			"'MINUS'", "'SETS'", "'SHOW'", "'SKEWED'", "'SOME'", "'SORT'", "'SORTED'", 
			"'START'", "'STATISTICS'", "'STORED'", "'STRATIFY'", "'STRUCT'", "'TABLE'", 
			"'TABLES'", "'TABLESAMPLE'", "'TBLPROPERTIES'", null, "'TERMINATED'", 
			"'THEN'", "'TO'", "'TOUCH'", "'TRAILING'", "'TRANSACTION'", "'TRANSACTIONS'", 
			"'TRANSFORM'", "'TRUE'", "'TRUNCATE'", "'UNARCHIVE'", "'UNBOUNDED'", 
			"'UNCACHE'", "'UNION'", "'UNIQUE'", "'UNLOCK'", "'UNSET'", "'USE'", "'USER'", 
			"'USING'", "'VALUES'", "'VIEW'", "'WEEK'", "'WEEKS'", "'WHEN'", "'WHERE'", 
			"'WINDOW'", "'WITH'", "'YEAR'", "'YEARS'", null, "'<=>'", "'<>'", "'!='", 
			"'<'", null, "'>'", null, "'+'", "'-'", "'*'", "'/'", "'%'", "'DIV'", 
			"'~'", "'&'", "'|'", "'||'", "'^'", null, null, null, null, null, null, 
			null, null, null, null, null, "'/**/'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, "ADD", 
			"AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", "ANY", "ARCHIVE", 
			"ARRAY", "AS", "ASC", "AT", "AUTHORIZATION", "BETWEEN", "BOTH", "BUCKET", 
			"BUCKETS", "BY", "CACHE", "CASCADE", "CASE", "CAST", "CHANGE", "CHECK", 
			"CLEAR", "CLUSTER", "CLUSTERED", "CODEGEN", "COLLATE", "COLLECTION", 
			"COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMPACT", "COMPACTIONS", "COMPUTE", 
			"CONCATENATE", "CONSTRAINT", "COST", "CREATE", "CROSS", "CUBE", "CURRENT", 
			"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", 
			"DATA", "DATABASE", "DATABASES", "DAY", "DAYS", "DBPROPERTIES", "DEFINED", 
			"DELETE", "DELIMITED", "DESC", "DESCRIBE", "DFS", "DIRECTORIES", "DIRECTORY", 
			"DISTINCT", "DISTRIBUTE", "DROP", "ELSE", "END", "ESCAPED", "EXCEPT", 
			"EXCHANGE", "EXISTS", "EXPLAIN", "EXPORT", "EXTENDED", "EXTERNAL", "EXTRACT", 
			"FALSE", "FETCH", "FIELDS", "FILEFORMAT", "FIRST", "FOLLOWING", "FOR", 
			"FOREIGN", "FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION", "FUNCTIONS", 
			"GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "HOUR", "HOURS", "IF", 
			"IGNORE", "IMPORT", "IN", "INDEX", "INDEXES", "INNER", "INPATH", "INPUTFORMAT", 
			"INSERT", "INTERSECT", "INTERVAL", "INTO", "IS", "ITEMS", "JOIN", "KEYS", 
			"LAST", "LATERAL", "LAZY", "LEADING", "LEFT", "LIKE", "LIMIT", "LINES", 
			"LIST", "LOAD", "LOCAL", "LOCATION", "LOCK", "LOCKS", "LOGICAL", "MACRO", 
			"MAP", "MICROSECOND", "MICROSECONDS", "MILLISECOND", "MILLISECONDS", 
			"MINUTE", "MINUTES", "MONTH", "MONTHS", "MSCK", "NATURAL", "NO", "NOT", 
			"NULL", "NULLS", "OF", "ON", "ONLY", "OPTION", "OPTIONS", "OR", "ORDER", 
			"OUT", "OUTER", "OUTPUTFORMAT", "OVER", "OVERLAPS", "OVERWRITE", "PARTITION", 
			"PARTITIONED", "PARTITIONS", "PERCENTLIT", "PIVOT", "POSITION", "PRECEDING", 
			"PRIMARY", "PRINCIPALS", "PURGE", "QUERY", "RANGE", "RECORDREADER", "RECORDWRITER", 
			"RECOVER", "REDUCE", "REFERENCES", "REFRESH", "RENAME", "REPAIR", "REPLACE", 
			"RESET", "RESTRICT", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", 
			"ROLLUP", "ROW", "ROWS", "SCHEMA", "SECOND", "SECONDS", "SELECT", "SEMI", 
			"SEPARATED", "SERDE", "SERDEPROPERTIES", "SESSION_USER", "SET", "SETMINUS", 
			"SETS", "SHOW", "SKEWED", "SOME", "SORT", "SORTED", "START", "STATISTICS", 
			"STORED", "STRATIFY", "STRUCT", "TABLE", "TABLES", "TABLESAMPLE", "TBLPROPERTIES", 
			"TEMPORARY", "TERMINATED", "THEN", "TO", "TOUCH", "TRAILING", "TRANSACTION", 
			"TRANSACTIONS", "TRANSFORM", "TRUE", "TRUNCATE", "UNARCHIVE", "UNBOUNDED", 
			"UNCACHE", "UNION", "UNIQUE", "UNLOCK", "UNSET", "USE", "USER", "USING", 
			"VALUES", "VIEW", "WEEK", "WEEKS", "WHEN", "WHERE", "WINDOW", "WITH", 
			"YEAR", "YEARS", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", 
			"PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "DIV", "TILDE", "AMPERSAND", 
			"PIPE", "CONCAT_PIPE", "HAT", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", 
			"TINYINT_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", 
			"BIGDECIMAL_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
			"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	  /**
	   * When false, INTERSECT is given the greater precedence over the other set
	   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
	   */
	  public boolean legacy_setops_precedence_enbled = false;

	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
	   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
	   * which is not a digit or letter or underscore.
	   */
	  public boolean isValidDecimal() {
	    int nextChar = _input.LA(1);
	    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
	      nextChar == '_') {
	      return false;
	    } else {
	      return true;
	    }
	  }

	  /**
	   * When true, ANSI SQL parsing mode is enabled.
	   */
	  public boolean ansi = false;

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(224);
			statement();
			setState(225);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(227);
			namedExpression();
			setState(228);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
			tableIdentifier();
			setState(231);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleMultipartIdentifierContext extends ParserRuleContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleMultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleMultipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleMultipartIdentifierContext singleMultipartIdentifier() throws RecognitionException {
		SingleMultipartIdentifierContext _localctx = new SingleMultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleMultipartIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(233);
			multipartIdentifier();
			setState(234);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleFunctionIdentifierContext extends ParserRuleContext {
		public FunctionIdentifierContext functionIdentifier() {
			return getRuleContext(FunctionIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleFunctionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleFunctionIdentifierContext singleFunctionIdentifier() throws RecognitionException {
		SingleFunctionIdentifierContext _localctx = new SingleFunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleFunctionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(236);
			functionIdentifier();
			setState(237);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(239);
			dataType();
			setState(240);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableSchemaContext extends ParserRuleContext {
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableSchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableSchema; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleTableSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableSchemaContext singleTableSchema() throws RecognitionException {
		SingleTableSchemaContext _localctx = new SingleTableSchemaContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_singleTableSchema);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(242);
			colTypeList();
			setState(243);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropDatabaseContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public DatabaseContext database() {
			return getRuleContext(DatabaseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDatabaseContext extends StatementContext {
		public DatabaseContext database() {
			return getRuleContext(DatabaseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterViewQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterViewQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UseContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTempViewUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTempViewUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTableContext extends StatementContext {
		public TableIdentifierContext from;
		public TableIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFailNativeCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFailNativeCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitClearCache(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitClearCache(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRecoverPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRecoverPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRecoverPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepairTableContext extends StatementContext {
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepairTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepairTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepairTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitManageResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitManageResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateDatabaseContext extends StatementContext {
		public Token comment;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public DatabaseContext database() {
			return getRuleContext(DatabaseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CreateDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateHiveTableContext extends StatementContext {
		public ColTypeListContext columns;
		public Token comment;
		public ColTypeListContext partitionColumns;
		public IdentifierListContext partitionColumnNames;
		public TablePropertyListContext tableProps;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<SkewSpecContext> skewSpec() {
			return getRuleContexts(SkewSpecContext.class);
		}
		public SkewSpecContext skewSpec(int i) {
			return getRuleContext(SkewSpecContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public List<TerminalNode> COMMENT() { return getTokens(SqlBaseParser.COMMENT); }
		public TerminalNode COMMENT(int i) {
			return getToken(SqlBaseParser.COMMENT, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public List<IdentifierListContext> identifierList() {
			return getRuleContexts(IdentifierListContext.class);
		}
		public IdentifierListContext identifierList(int i) {
			return getRuleContext(IdentifierListContext.class,i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateHiveTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateHiveTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateHiveTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateHiveTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTableContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetDatabasePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public DatabaseContext database() {
			return getRuleContext(DatabaseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetDatabasePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetDatabaseProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetDatabaseProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetDatabaseProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public TablePropertyListContext options;
		public TransformListContext partitioning;
		public Token comment;
		public TablePropertyListContext tableProps;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<TerminalNode> OPTIONS() { return getTokens(SqlBaseParser.OPTIONS); }
		public TerminalNode OPTIONS(int i) {
			return getToken(SqlBaseParser.OPTIONS, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<TerminalNode> COMMENT() { return getTokens(SqlBaseParser.COMMENT); }
		public TerminalNode COMMENT(int i) {
			return getToken(SqlBaseParser.COMMENT, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public List<TransformListContext> transformList() {
			return getRuleContexts(TransformListContext.class);
		}
		public TransformListContext transformList(int i) {
			return getRuleContext(TransformListContext.class,i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DmlStatementContext extends StatementContext {
		public DmlStatementNoWithContext dmlStatementNoWith() {
			return getRuleContext(DmlStatementNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public DmlStatementContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDmlStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDmlStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDmlStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeTableContext extends StatementContext {
		public Token option;
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUncacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUncacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoadData(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLoadData(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ChangeColumnContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public ChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterChangeColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitChangeColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitChangeColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeQueryContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public DescribeQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableSerDe(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableSerDe(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTablePartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTablePartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowDatabasesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowDatabasesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowDatabases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowDatabases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowDatabases(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTblPropertiesContext extends StatementContext {
		public TableIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTblProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTblProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CacheTableContext extends StatementContext {
		public TablePropertyListContext options;
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTableColumnsContext extends StatementContext {
		public ColTypeListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public AddTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTableColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddTableColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(873);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(245);
				query();
				}
				break;
			case 2:
				_localctx = new DmlStatementContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(246);
					ctes();
					}
				}

				setState(249);
				dmlStatementNoWith();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(250);
				match(USE);
				setState(251);
				((UseContext)_localctx).db = identifier();
				}
				break;
			case 4:
				_localctx = new CreateDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(252);
				match(CREATE);
				setState(253);
				database();
				setState(257);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
				case 1:
					{
					setState(254);
					match(IF);
					setState(255);
					match(NOT);
					setState(256);
					match(EXISTS);
					}
					break;
				}
				setState(259);
				identifier();
				setState(262);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(260);
					match(COMMENT);
					setState(261);
					((CreateDatabaseContext)_localctx).comment = match(STRING);
					}
				}

				setState(265);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(264);
					locationSpec();
					}
				}

				setState(270);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(267);
					match(WITH);
					setState(268);
					match(DBPROPERTIES);
					setState(269);
					tablePropertyList();
					}
				}

				}
				break;
			case 5:
				_localctx = new SetDatabasePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(272);
				match(ALTER);
				setState(273);
				database();
				setState(274);
				identifier();
				setState(275);
				match(SET);
				setState(276);
				match(DBPROPERTIES);
				setState(277);
				tablePropertyList();
				}
				break;
			case 6:
				_localctx = new DropDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(279);
				match(DROP);
				setState(280);
				database();
				setState(283);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
				case 1:
					{
					setState(281);
					match(IF);
					setState(282);
					match(EXISTS);
					}
					break;
				}
				setState(285);
				identifier();
				setState(287);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(286);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 7:
				_localctx = new ShowDatabasesContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(289);
				match(SHOW);
				setState(290);
				match(DATABASES);
				setState(295);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(292);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(291);
						match(LIKE);
						}
					}

					setState(294);
					((ShowDatabasesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 8:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(297);
				createTableHeader();
				setState(302);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(298);
					match(T__0);
					setState(299);
					colTypeList();
					setState(300);
					match(T__1);
					}
				}

				setState(304);
				tableProvider();
				setState(318);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CLUSTERED || _la==COMMENT || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (LOCATION - 137)) | (1L << (OPTIONS - 137)) | (1L << (PARTITIONED - 137)))) != 0) || _la==TBLPROPERTIES) {
					{
					setState(316);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case OPTIONS:
						{
						{
						setState(305);
						match(OPTIONS);
						setState(306);
						((CreateTableContext)_localctx).options = tablePropertyList();
						}
						}
						break;
					case PARTITIONED:
						{
						{
						setState(307);
						match(PARTITIONED);
						setState(308);
						match(BY);
						setState(309);
						((CreateTableContext)_localctx).partitioning = transformList();
						}
						}
						break;
					case CLUSTERED:
						{
						setState(310);
						bucketSpec();
						}
						break;
					case LOCATION:
						{
						setState(311);
						locationSpec();
						}
						break;
					case COMMENT:
						{
						{
						setState(312);
						match(COMMENT);
						setState(313);
						((CreateTableContext)_localctx).comment = match(STRING);
						}
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(314);
						match(TBLPROPERTIES);
						setState(315);
						((CreateTableContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(320);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(325);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0 || _la==AS || _la==FROM || _la==MAP || ((((_la - 185)) & ~0x3f) == 0 && ((1L << (_la - 185)) & ((1L << (REDUCE - 185)) | (1L << (SELECT - 185)) | (1L << (TABLE - 185)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(322);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(321);
						match(AS);
						}
					}

					setState(324);
					query();
					}
				}

				}
				break;
			case 9:
				_localctx = new CreateHiveTableContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(327);
				createTableHeader();
				setState(332);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(328);
					match(T__0);
					setState(329);
					((CreateHiveTableContext)_localctx).columns = colTypeList();
					setState(330);
					match(T__1);
					}
					break;
				}
				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CLUSTERED || _la==COMMENT || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (LOCATION - 137)) | (1L << (PARTITIONED - 137)) | (1L << (ROW - 137)))) != 0) || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (SKEWED - 215)) | (1L << (STORED - 215)) | (1L << (TBLPROPERTIES - 215)))) != 0)) {
					{
					setState(354);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						{
						setState(334);
						match(COMMENT);
						setState(335);
						((CreateHiveTableContext)_localctx).comment = match(STRING);
						}
						}
						break;
					case PARTITIONED:
						{
						setState(345);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
						case 1:
							{
							setState(336);
							match(PARTITIONED);
							setState(337);
							match(BY);
							setState(338);
							match(T__0);
							setState(339);
							((CreateHiveTableContext)_localctx).partitionColumns = colTypeList();
							setState(340);
							match(T__1);
							}
							break;
						case 2:
							{
							setState(342);
							match(PARTITIONED);
							setState(343);
							match(BY);
							setState(344);
							((CreateHiveTableContext)_localctx).partitionColumnNames = identifierList();
							}
							break;
						}
						}
						break;
					case CLUSTERED:
						{
						setState(347);
						bucketSpec();
						}
						break;
					case SKEWED:
						{
						setState(348);
						skewSpec();
						}
						break;
					case ROW:
						{
						setState(349);
						rowFormat();
						}
						break;
					case STORED:
						{
						setState(350);
						createFileFormat();
						}
						break;
					case LOCATION:
						{
						setState(351);
						locationSpec();
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(352);
						match(TBLPROPERTIES);
						setState(353);
						((CreateHiveTableContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(358);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(363);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0 || _la==AS || _la==FROM || _la==MAP || ((((_la - 185)) & ~0x3f) == 0 && ((1L << (_la - 185)) & ((1L << (REDUCE - 185)) | (1L << (SELECT - 185)) | (1L << (TABLE - 185)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(360);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(359);
						match(AS);
						}
					}

					setState(362);
					query();
					}
				}

				}
				break;
			case 10:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(365);
				match(CREATE);
				setState(366);
				match(TABLE);
				setState(370);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					{
					setState(367);
					match(IF);
					setState(368);
					match(NOT);
					setState(369);
					match(EXISTS);
					}
					break;
				}
				setState(372);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(373);
				match(LIKE);
				setState(374);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(376);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(375);
					locationSpec();
					}
				}

				}
				break;
			case 11:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(378);
				match(ANALYZE);
				setState(379);
				match(TABLE);
				setState(380);
				tableIdentifier();
				setState(382);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(381);
					partitionSpec();
					}
				}

				setState(384);
				match(COMPUTE);
				setState(385);
				match(STATISTICS);
				setState(393);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
				case 1:
					{
					setState(386);
					identifier();
					}
					break;
				case 2:
					{
					setState(387);
					match(FOR);
					setState(388);
					match(COLUMNS);
					setState(389);
					identifierSeq();
					}
					break;
				case 3:
					{
					setState(390);
					match(FOR);
					setState(391);
					match(ALL);
					setState(392);
					match(COLUMNS);
					}
					break;
				}
				}
				break;
			case 12:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(395);
				match(ALTER);
				setState(396);
				match(TABLE);
				setState(397);
				tableIdentifier();
				setState(398);
				match(ADD);
				setState(399);
				match(COLUMNS);
				setState(400);
				match(T__0);
				setState(401);
				((AddTableColumnsContext)_localctx).columns = colTypeList();
				setState(402);
				match(T__1);
				}
				break;
			case 13:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(404);
				match(ALTER);
				setState(405);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(406);
				((RenameTableContext)_localctx).from = tableIdentifier();
				setState(407);
				match(RENAME);
				setState(408);
				match(TO);
				setState(409);
				((RenameTableContext)_localctx).to = tableIdentifier();
				}
				break;
			case 14:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(411);
				match(ALTER);
				setState(412);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(413);
				tableIdentifier();
				setState(414);
				match(SET);
				setState(415);
				match(TBLPROPERTIES);
				setState(416);
				tablePropertyList();
				}
				break;
			case 15:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(418);
				match(ALTER);
				setState(419);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(420);
				tableIdentifier();
				setState(421);
				match(UNSET);
				setState(422);
				match(TBLPROPERTIES);
				setState(425);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(423);
					match(IF);
					setState(424);
					match(EXISTS);
					}
				}

				setState(427);
				tablePropertyList();
				}
				break;
			case 16:
				_localctx = new ChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(429);
				match(ALTER);
				setState(430);
				match(TABLE);
				setState(431);
				tableIdentifier();
				setState(433);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(432);
					partitionSpec();
					}
				}

				setState(435);
				match(CHANGE);
				setState(437);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
				case 1:
					{
					setState(436);
					match(COLUMN);
					}
					break;
				}
				setState(439);
				identifier();
				setState(440);
				colType();
				setState(442);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER || _la==FIRST) {
					{
					setState(441);
					colPosition();
					}
				}

				}
				break;
			case 17:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(444);
				match(ALTER);
				setState(445);
				match(TABLE);
				setState(446);
				tableIdentifier();
				setState(448);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(447);
					partitionSpec();
					}
				}

				setState(450);
				match(SET);
				setState(451);
				match(SERDE);
				setState(452);
				match(STRING);
				setState(456);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(453);
					match(WITH);
					setState(454);
					match(SERDEPROPERTIES);
					setState(455);
					tablePropertyList();
					}
				}

				}
				break;
			case 18:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(458);
				match(ALTER);
				setState(459);
				match(TABLE);
				setState(460);
				tableIdentifier();
				setState(462);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(461);
					partitionSpec();
					}
				}

				setState(464);
				match(SET);
				setState(465);
				match(SERDEPROPERTIES);
				setState(466);
				tablePropertyList();
				}
				break;
			case 19:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(468);
				match(ALTER);
				setState(469);
				match(TABLE);
				setState(470);
				tableIdentifier();
				setState(471);
				match(ADD);
				setState(475);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(472);
					match(IF);
					setState(473);
					match(NOT);
					setState(474);
					match(EXISTS);
					}
				}

				setState(478); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(477);
					partitionSpecLocation();
					}
					}
					setState(480); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 20:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(482);
				match(ALTER);
				setState(483);
				match(VIEW);
				setState(484);
				tableIdentifier();
				setState(485);
				match(ADD);
				setState(489);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(486);
					match(IF);
					setState(487);
					match(NOT);
					setState(488);
					match(EXISTS);
					}
				}

				setState(492); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(491);
					partitionSpec();
					}
					}
					setState(494); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 21:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(496);
				match(ALTER);
				setState(497);
				match(TABLE);
				setState(498);
				tableIdentifier();
				setState(499);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(500);
				match(RENAME);
				setState(501);
				match(TO);
				setState(502);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 22:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(504);
				match(ALTER);
				setState(505);
				match(TABLE);
				setState(506);
				tableIdentifier();
				setState(507);
				match(DROP);
				setState(510);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(508);
					match(IF);
					setState(509);
					match(EXISTS);
					}
				}

				setState(512);
				partitionSpec();
				setState(517);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(513);
					match(T__2);
					setState(514);
					partitionSpec();
					}
					}
					setState(519);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(520);
					match(PURGE);
					}
				}

				}
				break;
			case 23:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(523);
				match(ALTER);
				setState(524);
				match(VIEW);
				setState(525);
				tableIdentifier();
				setState(526);
				match(DROP);
				setState(529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(527);
					match(IF);
					setState(528);
					match(EXISTS);
					}
				}

				setState(531);
				partitionSpec();
				setState(536);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(532);
					match(T__2);
					setState(533);
					partitionSpec();
					}
					}
					setState(538);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 24:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(539);
				match(ALTER);
				setState(540);
				match(TABLE);
				setState(541);
				tableIdentifier();
				setState(543);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(542);
					partitionSpec();
					}
				}

				setState(545);
				match(SET);
				setState(546);
				locationSpec();
				}
				break;
			case 25:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(548);
				match(ALTER);
				setState(549);
				match(TABLE);
				setState(550);
				tableIdentifier();
				setState(551);
				match(RECOVER);
				setState(552);
				match(PARTITIONS);
				}
				break;
			case 26:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(554);
				match(DROP);
				setState(555);
				match(TABLE);
				setState(558);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
				case 1:
					{
					setState(556);
					match(IF);
					setState(557);
					match(EXISTS);
					}
					break;
				}
				setState(560);
				tableIdentifier();
				setState(562);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(561);
					match(PURGE);
					}
				}

				}
				break;
			case 27:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(564);
				match(DROP);
				setState(565);
				match(VIEW);
				setState(568);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
				case 1:
					{
					setState(566);
					match(IF);
					setState(567);
					match(EXISTS);
					}
					break;
				}
				setState(570);
				tableIdentifier();
				}
				break;
			case 28:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(571);
				match(CREATE);
				setState(574);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(572);
					match(OR);
					setState(573);
					match(REPLACE);
					}
				}

				setState(580);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(577);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(576);
						match(GLOBAL);
						}
					}

					setState(579);
					match(TEMPORARY);
					}
				}

				setState(582);
				match(VIEW);
				setState(586);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
				case 1:
					{
					setState(583);
					match(IF);
					setState(584);
					match(NOT);
					setState(585);
					match(EXISTS);
					}
					break;
				}
				setState(588);
				tableIdentifier();
				setState(590);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(589);
					identifierCommentList();
					}
				}

				setState(594);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(592);
					match(COMMENT);
					setState(593);
					match(STRING);
					}
				}

				setState(599);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(596);
					match(PARTITIONED);
					setState(597);
					match(ON);
					setState(598);
					identifierList();
					}
				}

				setState(603);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(601);
					match(TBLPROPERTIES);
					setState(602);
					tablePropertyList();
					}
				}

				setState(605);
				match(AS);
				setState(606);
				query();
				}
				break;
			case 29:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(608);
				match(CREATE);
				setState(611);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(609);
					match(OR);
					setState(610);
					match(REPLACE);
					}
				}

				setState(614);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(613);
					match(GLOBAL);
					}
				}

				setState(616);
				match(TEMPORARY);
				setState(617);
				match(VIEW);
				setState(618);
				tableIdentifier();
				setState(623);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(619);
					match(T__0);
					setState(620);
					colTypeList();
					setState(621);
					match(T__1);
					}
				}

				setState(625);
				tableProvider();
				setState(628);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(626);
					match(OPTIONS);
					setState(627);
					tablePropertyList();
					}
				}

				}
				break;
			case 30:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(630);
				match(ALTER);
				setState(631);
				match(VIEW);
				setState(632);
				tableIdentifier();
				setState(634);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(633);
					match(AS);
					}
				}

				setState(636);
				query();
				}
				break;
			case 31:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(638);
				match(CREATE);
				setState(641);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(639);
					match(OR);
					setState(640);
					match(REPLACE);
					}
				}

				setState(644);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(643);
					match(TEMPORARY);
					}
				}

				setState(646);
				match(FUNCTION);
				setState(650);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
				case 1:
					{
					setState(647);
					match(IF);
					setState(648);
					match(NOT);
					setState(649);
					match(EXISTS);
					}
					break;
				}
				setState(652);
				qualifiedName();
				setState(653);
				match(AS);
				setState(654);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(664);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(655);
					match(USING);
					setState(656);
					resource();
					setState(661);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(657);
						match(T__2);
						setState(658);
						resource();
						}
						}
						setState(663);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 32:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(666);
				match(DROP);
				setState(668);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(667);
					match(TEMPORARY);
					}
				}

				setState(670);
				match(FUNCTION);
				setState(673);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
				case 1:
					{
					setState(671);
					match(IF);
					setState(672);
					match(EXISTS);
					}
					break;
				}
				setState(675);
				qualifiedName();
				}
				break;
			case 33:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(676);
				match(EXPLAIN);
				setState(678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CODEGEN || _la==COST || ((((_la - 85)) & ~0x3f) == 0 && ((1L << (_la - 85)) & ((1L << (EXTENDED - 85)) | (1L << (FORMATTED - 85)) | (1L << (LOGICAL - 85)))) != 0)) {
					{
					setState(677);
					_la = _input.LA(1);
					if ( !(_la==CODEGEN || _la==COST || ((((_la - 85)) & ~0x3f) == 0 && ((1L << (_la - 85)) & ((1L << (EXTENDED - 85)) | (1L << (FORMATTED - 85)) | (1L << (LOGICAL - 85)))) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(680);
				statement();
				}
				break;
			case 34:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(681);
				match(SHOW);
				setState(682);
				match(TABLES);
				setState(685);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(683);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(684);
					((ShowTablesContext)_localctx).db = identifier();
					}
				}

				setState(691);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(688);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(687);
						match(LIKE);
						}
					}

					setState(690);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 35:
				_localctx = new ShowTableContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(693);
				match(SHOW);
				setState(694);
				match(TABLE);
				setState(695);
				match(EXTENDED);
				setState(698);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(696);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(697);
					((ShowTableContext)_localctx).db = identifier();
					}
				}

				setState(700);
				match(LIKE);
				setState(701);
				((ShowTableContext)_localctx).pattern = match(STRING);
				setState(703);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(702);
					partitionSpec();
					}
				}

				}
				break;
			case 36:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(705);
				match(SHOW);
				setState(706);
				match(TBLPROPERTIES);
				setState(707);
				((ShowTblPropertiesContext)_localctx).table = tableIdentifier();
				setState(712);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(708);
					match(T__0);
					setState(709);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(710);
					match(T__1);
					}
				}

				}
				break;
			case 37:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(714);
				match(SHOW);
				setState(715);
				match(COLUMNS);
				setState(716);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(717);
				tableIdentifier();
				setState(720);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(718);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(719);
					((ShowColumnsContext)_localctx).db = identifier();
					}
				}

				}
				break;
			case 38:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(722);
				match(SHOW);
				setState(723);
				match(PARTITIONS);
				setState(724);
				tableIdentifier();
				setState(726);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(725);
					partitionSpec();
					}
				}

				}
				break;
			case 39:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(728);
				match(SHOW);
				setState(730);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
				case 1:
					{
					setState(729);
					identifier();
					}
					break;
				}
				setState(732);
				match(FUNCTIONS);
				setState(740);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
				case 1:
					{
					setState(734);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
					case 1:
						{
						setState(733);
						match(LIKE);
						}
						break;
					}
					setState(738);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
					case 1:
						{
						setState(736);
						qualifiedName();
						}
						break;
					case 2:
						{
						setState(737);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					}
					}
					break;
				}
				}
				break;
			case 40:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(742);
				match(SHOW);
				setState(743);
				match(CREATE);
				setState(744);
				match(TABLE);
				setState(745);
				tableIdentifier();
				}
				break;
			case 41:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(746);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(747);
				match(FUNCTION);
				setState(749);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(748);
					match(EXTENDED);
					}
					break;
				}
				setState(751);
				describeFuncName();
				}
				break;
			case 42:
				_localctx = new DescribeDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(752);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(753);
				database();
				setState(755);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
				case 1:
					{
					setState(754);
					match(EXTENDED);
					}
					break;
				}
				setState(757);
				identifier();
				}
				break;
			case 43:
				_localctx = new DescribeTableContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(759);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(761);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
				case 1:
					{
					setState(760);
					match(TABLE);
					}
					break;
				}
				setState(764);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
				case 1:
					{
					setState(763);
					((DescribeTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(766);
				tableIdentifier();
				setState(768);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
				case 1:
					{
					setState(767);
					partitionSpec();
					}
					break;
				}
				setState(771);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(770);
					describeColName();
					}
					break;
				}
				}
				break;
			case 44:
				_localctx = new DescribeQueryContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(773);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(775);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==QUERY) {
					{
					setState(774);
					match(QUERY);
					}
				}

				setState(777);
				query();
				}
				break;
			case 45:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(778);
				match(REFRESH);
				setState(779);
				match(TABLE);
				setState(780);
				tableIdentifier();
				}
				break;
			case 46:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(781);
				match(REFRESH);
				setState(789);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
				case 1:
					{
					setState(782);
					match(STRING);
					}
					break;
				case 2:
					{
					setState(786);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,84,_ctx);
					while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(783);
							matchWildcard();
							}
							} 
						}
						setState(788);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,84,_ctx);
					}
					}
					break;
				}
				}
				break;
			case 47:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(791);
				match(CACHE);
				setState(793);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(792);
					match(LAZY);
					}
				}

				setState(795);
				match(TABLE);
				setState(796);
				tableIdentifier();
				setState(799);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(797);
					match(OPTIONS);
					setState(798);
					((CacheTableContext)_localctx).options = tablePropertyList();
					}
				}

				setState(805);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0 || _la==AS || _la==FROM || _la==MAP || ((((_la - 185)) & ~0x3f) == 0 && ((1L << (_la - 185)) & ((1L << (REDUCE - 185)) | (1L << (SELECT - 185)) | (1L << (TABLE - 185)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(802);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(801);
						match(AS);
						}
					}

					setState(804);
					query();
					}
				}

				}
				break;
			case 48:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(807);
				match(UNCACHE);
				setState(808);
				match(TABLE);
				setState(811);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
				case 1:
					{
					setState(809);
					match(IF);
					setState(810);
					match(EXISTS);
					}
					break;
				}
				setState(813);
				tableIdentifier();
				}
				break;
			case 49:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(814);
				match(CLEAR);
				setState(815);
				match(CACHE);
				}
				break;
			case 50:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(816);
				match(LOAD);
				setState(817);
				match(DATA);
				setState(819);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(818);
					match(LOCAL);
					}
				}

				setState(821);
				match(INPATH);
				setState(822);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(824);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(823);
					match(OVERWRITE);
					}
				}

				setState(826);
				match(INTO);
				setState(827);
				match(TABLE);
				setState(828);
				tableIdentifier();
				setState(830);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(829);
					partitionSpec();
					}
				}

				}
				break;
			case 51:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(832);
				match(TRUNCATE);
				setState(833);
				match(TABLE);
				setState(834);
				tableIdentifier();
				setState(836);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(835);
					partitionSpec();
					}
				}

				}
				break;
			case 52:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(838);
				match(MSCK);
				setState(839);
				match(REPAIR);
				setState(840);
				match(TABLE);
				setState(841);
				tableIdentifier();
				}
				break;
			case 53:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(842);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(843);
				identifier();
				setState(847);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,95,_ctx);
				while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(844);
						matchWildcard();
						}
						} 
					}
					setState(849);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,95,_ctx);
				}
				}
				break;
			case 54:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(850);
				match(SET);
				setState(851);
				match(ROLE);
				setState(855);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(852);
						matchWildcard();
						}
						} 
					}
					setState(857);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				}
				}
				break;
			case 55:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(858);
				match(SET);
				setState(862);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(859);
						matchWildcard();
						}
						} 
					}
					setState(864);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				}
				}
				break;
			case 56:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(865);
				match(RESET);
				}
				break;
			case 57:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(866);
				unsupportedHiveNativeCommands();
				setState(870);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(867);
						matchWildcard();
						}
						} 
					}
					setState(872);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsupportedHiveNativeCommands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(1045);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,107,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(875);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(876);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(877);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(878);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(879);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(881);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
				case 1:
					{
					setState(880);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(883);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(885);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
				case 1:
					{
					setState(884);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(887);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(888);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(889);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(890);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(892);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
				case 1:
					{
					setState(891);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(894);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(895);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(896);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(897);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(898);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(899);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(900);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(901);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(902);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(903);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(904);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(905);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(906);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(907);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(908);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(909);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(910);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(911);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(912);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(913);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(914);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(915);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(916);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(917);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(918);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(919);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(920);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(921);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(922);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(923);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(924);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(925);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(926);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(927);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(928);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(929);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(930);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(931);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(932);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(933);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(934);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(935);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(936);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(937);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(938);
				tableIdentifier();
				setState(939);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(940);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(942);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(943);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(944);
				tableIdentifier();
				setState(945);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(946);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(948);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(949);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(950);
				tableIdentifier();
				setState(951);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(952);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(954);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(955);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(956);
				tableIdentifier();
				setState(957);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(958);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(960);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(961);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(962);
				tableIdentifier();
				setState(963);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(964);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(966);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(967);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(968);
				tableIdentifier();
				setState(969);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(970);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(971);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(972);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(974);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(975);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(976);
				tableIdentifier();
				setState(977);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(978);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(979);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(981);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(982);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(983);
				tableIdentifier();
				setState(984);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(985);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(987);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(988);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(989);
				tableIdentifier();
				setState(990);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(991);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(993);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(994);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(995);
				tableIdentifier();
				setState(996);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(997);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(999);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1000);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1001);
				tableIdentifier();
				setState(1002);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(1004);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1005);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1006);
				tableIdentifier();
				setState(1008);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1007);
					partitionSpec();
					}
				}

				setState(1010);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(1012);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1013);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1014);
				tableIdentifier();
				setState(1016);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1015);
					partitionSpec();
					}
				}

				setState(1018);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(1020);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1021);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1022);
				tableIdentifier();
				setState(1024);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1023);
					partitionSpec();
					}
				}

				setState(1026);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1027);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(1029);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1030);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1031);
				tableIdentifier();
				setState(1033);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1032);
					partitionSpec();
					}
				}

				setState(1035);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(1036);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(1038);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(1039);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(1040);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(1041);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(1042);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(1043);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DELETE);
				setState(1044);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(FROM);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1047);
			match(CREATE);
			setState(1049);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(1048);
				match(TEMPORARY);
				}
			}

			setState(1052);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(1051);
				match(EXTERNAL);
				}
			}

			setState(1054);
			match(TABLE);
			setState(1058);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,110,_ctx) ) {
			case 1:
				{
				setState(1055);
				match(IF);
				setState(1056);
				match(NOT);
				setState(1057);
				match(EXISTS);
				}
				break;
			}
			setState(1060);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBucketSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBucketSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1062);
			match(CLUSTERED);
			setState(1063);
			match(BY);
			setState(1064);
			identifierList();
			setState(1068);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(1065);
				match(SORTED);
				setState(1066);
				match(BY);
				setState(1067);
				orderedIdentifierList();
				}
			}

			setState(1070);
			match(INTO);
			setState(1071);
			match(INTEGER_VALUE);
			setState(1072);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkewSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSkewSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1074);
			match(SKEWED);
			setState(1075);
			match(BY);
			setState(1076);
			identifierList();
			setState(1077);
			match(ON);
			setState(1080);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				{
				setState(1078);
				constantList();
				}
				break;
			case 2:
				{
				setState(1079);
				nestedConstantList();
				}
				break;
			}
			setState(1085);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
			case 1:
				{
				setState(1082);
				match(STORED);
				setState(1083);
				match(AS);
				setState(1084);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocationSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLocationSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1087);
			match(LOCATION);
			setState(1088);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1091);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1090);
				ctes();
				}
			}

			setState(1093);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertIntoContext extends ParserRuleContext {
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
	 
		public InsertIntoContext() { }
		public void copyFrom(InsertIntoContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InsertOverwriteHiveDirContext extends InsertIntoContext {
		public Token path;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public InsertOverwriteHiveDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteHiveDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteHiveDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertOverwriteHiveDir(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertOverwriteDirContext extends InsertIntoContext {
		public Token path;
		public TablePropertyListContext options;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public InsertOverwriteDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertOverwriteDir(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertOverwriteTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public InsertOverwriteTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertOverwriteTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertIntoTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public InsertIntoTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertIntoTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertIntoTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_insertInto);
		int _la;
		try {
			setState(1143);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				_localctx = new InsertOverwriteTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1095);
				match(INSERT);
				setState(1096);
				match(OVERWRITE);
				setState(1097);
				match(TABLE);
				setState(1098);
				tableIdentifier();
				setState(1105);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1099);
					partitionSpec();
					setState(1103);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1100);
						match(IF);
						setState(1101);
						match(NOT);
						setState(1102);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				_localctx = new InsertIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1107);
				match(INSERT);
				setState(1108);
				match(INTO);
				setState(1110);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,117,_ctx) ) {
				case 1:
					{
					setState(1109);
					match(TABLE);
					}
					break;
				}
				setState(1112);
				tableIdentifier();
				setState(1114);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1113);
					partitionSpec();
					}
				}

				}
				break;
			case 3:
				_localctx = new InsertOverwriteHiveDirContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1116);
				match(INSERT);
				setState(1117);
				match(OVERWRITE);
				setState(1119);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1118);
					match(LOCAL);
					}
				}

				setState(1121);
				match(DIRECTORY);
				setState(1122);
				((InsertOverwriteHiveDirContext)_localctx).path = match(STRING);
				setState(1124);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1123);
					rowFormat();
					}
				}

				setState(1127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(1126);
					createFileFormat();
					}
				}

				}
				break;
			case 4:
				_localctx = new InsertOverwriteDirContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1129);
				match(INSERT);
				setState(1130);
				match(OVERWRITE);
				setState(1132);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1131);
					match(LOCAL);
					}
				}

				setState(1134);
				match(DIRECTORY);
				setState(1136);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING) {
					{
					setState(1135);
					((InsertOverwriteDirContext)_localctx).path = match(STRING);
					}
				}

				setState(1138);
				tableProvider();
				setState(1141);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(1139);
					match(OPTIONS);
					setState(1140);
					((InsertOverwriteDirContext)_localctx).options = tablePropertyList();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpecLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpecLocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1145);
			partitionSpec();
			setState(1147);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1146);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1149);
			match(PARTITION);
			setState(1150);
			match(T__0);
			setState(1151);
			partitionVal();
			setState(1156);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1152);
				match(T__2);
				setState(1153);
				partitionVal();
				}
				}
				setState(1158);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1159);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionVal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionVal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1161);
			identifier();
			setState(1164);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1162);
				match(EQ);
				setState(1163);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatabaseContext extends ParserRuleContext {
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public DatabaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_database; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDatabase(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatabaseContext database() throws RecognitionException {
		DatabaseContext _localctx = new DatabaseContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_database);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1166);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFuncName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFuncName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_describeFuncName);
		try {
			setState(1173);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1168);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1169);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1170);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1171);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1172);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeColNameContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> nameParts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeColName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1175);
			((DescribeColNameContext)_localctx).identifier = identifier();
			((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
			setState(1180);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1176);
				match(T__3);
				setState(1177);
				((DescribeColNameContext)_localctx).identifier = identifier();
				((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
				}
				}
				setState(1182);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCtes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCtes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1183);
			match(WITH);
			setState(1184);
			namedQuery();
			setState(1189);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1185);
				match(T__2);
				setState(1186);
				namedQuery();
				}
				}
				setState(1191);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1192);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1194);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1193);
				match(AS);
				}
			}

			setState(1196);
			match(T__0);
			setState(1197);
			query();
			setState(1198);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProvider(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProvider(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1200);
			match(USING);
			setState(1201);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1203);
			match(T__0);
			setState(1204);
			tableProperty();
			setState(1209);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1205);
				match(T__2);
				setState(1206);
				tableProperty();
				}
				}
				setState(1211);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1212);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public TablePropertyValueContext value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TablePropertyValueContext tablePropertyValue() {
			return getRuleContext(TablePropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1214);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1219);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FALSE || ((((_la - 237)) & ~0x3f) == 0 && ((1L << (_la - 237)) & ((1L << (TRUE - 237)) | (1L << (EQ - 237)) | (1L << (STRING - 237)) | (1L << (INTEGER_VALUE - 237)) | (1L << (DECIMAL_VALUE - 237)))) != 0)) {
				{
				setState(1216);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1215);
					match(EQ);
					}
				}

				setState(1218);
				((TablePropertyContext)_localctx).value = tablePropertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1230);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,137,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1221);
				identifier();
				setState(1226);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1222);
					match(T__3);
					setState(1223);
					identifier();
					}
					}
					setState(1228);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1229);
				match(STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
		TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_tablePropertyValue);
		try {
			setState(1236);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1232);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1233);
				match(DECIMAL_VALUE);
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1234);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1235);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1238);
			match(T__0);
			setState(1239);
			constant();
			setState(1244);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1240);
				match(T__2);
				setState(1241);
				constant();
				}
				}
				setState(1246);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1247);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNestedConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNestedConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1249);
			match(T__0);
			setState(1250);
			constantList();
			setState(1255);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1251);
				match(T__2);
				setState(1252);
				constantList();
				}
				}
				setState(1257);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1258);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_createFileFormat);
		try {
			setState(1266);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,141,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1260);
				match(STORED);
				setState(1261);
				match(AS);
				setState(1262);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1263);
				match(STORED);
				setState(1264);
				match(BY);
				setState(1265);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGenericFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_fileFormat);
		try {
			setState(1273);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1268);
				match(INPUTFORMAT);
				setState(1269);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1270);
				match(OUTPUTFORMAT);
				setState(1271);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1272);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStorageHandler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStorageHandler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1275);
			match(STRING);
			setState(1279);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				{
				setState(1276);
				match(WITH);
				setState(1277);
				match(SERDEPROPERTIES);
				setState(1278);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1281);
			identifier();
			setState(1282);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DmlStatementNoWithContext extends ParserRuleContext {
		public DmlStatementNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dmlStatementNoWith; }
	 
		public DmlStatementNoWithContext() { }
		public void copyFrom(DmlStatementNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SingleInsertQueryContext extends DmlStatementNoWithContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public SingleInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MultiInsertQueryContext extends DmlStatementNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DmlStatementNoWithContext dmlStatementNoWith() throws RecognitionException {
		DmlStatementNoWithContext _localctx = new DmlStatementNoWithContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_dmlStatementNoWith);
		int _la;
		try {
			setState(1294);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1284);
				insertInto();
				setState(1285);
				queryTerm(0);
				setState(1286);
				queryOrganization();
				}
				break;
			case FROM:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1288);
				fromClause();
				setState(1290); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1289);
					multiInsertQueryBody();
					}
					}
					setState(1292); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==INSERT );
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryNoWithContext extends ParserRuleContext {
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
	 
		public QueryNoWithContext() { }
		public void copyFrom(QueryNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NoWithQueryContext extends QueryNoWithContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public NoWithQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNoWithQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNoWithQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNoWithQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryWithFromContext extends QueryNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<SelectStatementContext> selectStatement() {
			return getRuleContexts(SelectStatementContext.class);
		}
		public SelectStatementContext selectStatement(int i) {
			return getRuleContext(SelectStatementContext.class,i);
		}
		public QueryWithFromContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryWithFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryWithFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryWithFrom(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_queryNoWith);
		int _la;
		try {
			setState(1305);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,147,_ctx) ) {
			case 1:
				_localctx = new NoWithQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1296);
				queryTerm(0);
				setState(1297);
				queryOrganization();
				}
				break;
			case 2:
				_localctx = new QueryWithFromContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1299);
				fromClause();
				setState(1301); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1300);
					selectStatement();
					}
					}
					setState(1303); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==FROM || _la==MAP || _la==REDUCE || _la==SELECT );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1317);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1307);
				match(ORDER);
				setState(1308);
				match(BY);
				setState(1309);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1314);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1310);
					match(T__2);
					setState(1311);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1316);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTER) {
				{
				setState(1319);
				match(CLUSTER);
				setState(1320);
				match(BY);
				setState(1321);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1326);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1322);
					match(T__2);
					setState(1323);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1328);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1341);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTE) {
				{
				setState(1331);
				match(DISTRIBUTE);
				setState(1332);
				match(BY);
				setState(1333);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1338);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1334);
					match(T__2);
					setState(1335);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1340);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1353);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORT) {
				{
				setState(1343);
				match(SORT);
				setState(1344);
				match(BY);
				setState(1345);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1350);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1346);
					match(T__2);
					setState(1347);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1352);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1356);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WINDOW) {
				{
				setState(1355);
				windows();
				}
			}

			setState(1363);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(1358);
				match(LIMIT);
				setState(1361);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,157,_ctx) ) {
				case 1:
					{
					setState(1359);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(1360);
					((QueryOrganizationContext)_localctx).limit = expression();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQueryBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQueryBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_multiInsertQueryBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1365);
			insertInto();
			setState(1366);
			selectStatement();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectStatementContext extends ParserRuleContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public SelectStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectStatementContext selectStatement() throws RecognitionException {
		SelectStatementContext _localctx = new SelectStatementContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_selectStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1368);
			querySpecification();
			setState(1369);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 78;
		enterRecursionRule(_localctx, 78, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1372);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1397);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,163,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1395);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,162,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1374);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1375);
						if (!(legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "legacy_setops_precedence_enbled");
						setState(1376);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==INTERSECT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1378);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1377);
							setQuantifier();
							}
						}

						setState(1380);
						((SetOperationContext)_localctx).right = queryTerm(4);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1381);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1382);
						if (!(!legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
						setState(1383);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1385);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1384);
							setQuantifier();
							}
						}

						setState(1387);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 3:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1388);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1389);
						if (!(!legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
						setState(1390);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1392);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1391);
							setQuantifier();
							}
						}

						setState(1394);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1399);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,163,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_queryPrimary);
		try {
			setState(1408);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FROM:
			case MAP:
			case REDUCE:
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1400);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1401);
				match(TABLE);
				setState(1402);
				tableIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1403);
				inlineTable();
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1404);
				match(T__0);
				setState(1405);
				queryNoWith();
				setState(1406);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1410);
			expression();
			setState(1412);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1411);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1416);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1414);
				match(NULLS);
				setState(1415);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public BooleanExpressionContext where;
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public BooleanExpressionContext having;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(1511);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				{
				setState(1428);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1418);
					match(SELECT);
					setState(1419);
					((QuerySpecificationContext)_localctx).kind = match(TRANSFORM);
					setState(1420);
					match(T__0);
					setState(1421);
					namedExpressionSeq();
					setState(1422);
					match(T__1);
					}
					break;
				case MAP:
					{
					setState(1424);
					((QuerySpecificationContext)_localctx).kind = match(MAP);
					setState(1425);
					namedExpressionSeq();
					}
					break;
				case REDUCE:
					{
					setState(1426);
					((QuerySpecificationContext)_localctx).kind = match(REDUCE);
					setState(1427);
					namedExpressionSeq();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1431);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1430);
					((QuerySpecificationContext)_localctx).inRowFormat = rowFormat();
					}
				}

				setState(1435);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RECORDWRITER) {
					{
					setState(1433);
					match(RECORDWRITER);
					setState(1434);
					((QuerySpecificationContext)_localctx).recordWriter = match(STRING);
					}
				}

				setState(1437);
				match(USING);
				setState(1438);
				((QuerySpecificationContext)_localctx).script = match(STRING);
				setState(1451);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
				case 1:
					{
					setState(1439);
					match(AS);
					setState(1449);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
					case 1:
						{
						setState(1440);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1441);
						colTypeList();
						}
						break;
					case 3:
						{
						{
						setState(1442);
						match(T__0);
						setState(1445);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
						case 1:
							{
							setState(1443);
							identifierSeq();
							}
							break;
						case 2:
							{
							setState(1444);
							colTypeList();
							}
							break;
						}
						setState(1447);
						match(T__1);
						}
						}
						break;
					}
					}
					break;
				}
				setState(1454);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
				case 1:
					{
					setState(1453);
					((QuerySpecificationContext)_localctx).outRowFormat = rowFormat();
					}
					break;
				}
				setState(1458);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
				case 1:
					{
					setState(1456);
					match(RECORDREADER);
					setState(1457);
					((QuerySpecificationContext)_localctx).recordReader = match(STRING);
					}
					break;
				}
				setState(1461);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
				case 1:
					{
					setState(1460);
					fromClause();
					}
					break;
				}
				setState(1465);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
				case 1:
					{
					setState(1463);
					match(WHERE);
					setState(1464);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1489);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1467);
					((QuerySpecificationContext)_localctx).kind = match(SELECT);
					setState(1471);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
					while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(1468);
							((QuerySpecificationContext)_localctx).hint = hint();
							((QuerySpecificationContext)_localctx).hints.add(((QuerySpecificationContext)_localctx).hint);
							}
							} 
						}
						setState(1473);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
					}
					setState(1475);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
					case 1:
						{
						setState(1474);
						setQuantifier();
						}
						break;
					}
					setState(1477);
					namedExpressionSeq();
					setState(1479);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
					case 1:
						{
						setState(1478);
						fromClause();
						}
						break;
					}
					}
					break;
				case FROM:
					{
					setState(1481);
					fromClause();
					setState(1487);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,181,_ctx) ) {
					case 1:
						{
						setState(1482);
						((QuerySpecificationContext)_localctx).kind = match(SELECT);
						setState(1484);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
						case 1:
							{
							setState(1483);
							setQuantifier();
							}
							break;
						}
						setState(1486);
						namedExpressionSeq();
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1494);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
				while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1491);
						lateralView();
						}
						} 
					}
					setState(1496);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
				}
				setState(1499);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
				case 1:
					{
					setState(1497);
					match(WHERE);
					setState(1498);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(1502);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
				case 1:
					{
					setState(1501);
					aggregation();
					}
					break;
				}
				setState(1506);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,186,_ctx) ) {
				case 1:
					{
					setState(1504);
					match(HAVING);
					setState(1505);
					((QuerySpecificationContext)_localctx).having = booleanExpression(0);
					}
					break;
				}
				setState(1509);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
				case 1:
					{
					setState(1508);
					windows();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitHint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_hint);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1513);
			match(T__4);
			setState(1514);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(1521);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1516);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
					case 1:
						{
						setState(1515);
						match(T__2);
						}
						break;
					}
					setState(1518);
					((HintContext)_localctx).hintStatement = hintStatement();
					((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
					}
					} 
				}
				setState(1523);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
			}
			setState(1524);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHintStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHintStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitHintStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_hintStatement);
		int _la;
		try {
			setState(1539);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1526);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1527);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(1528);
				match(T__0);
				setState(1529);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(1534);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1530);
					match(T__2);
					setState(1531);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(1536);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1537);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public PivotClauseContext pivotClause() {
			return getRuleContext(PivotClauseContext.class,0);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1541);
			match(FROM);
			setState(1542);
			relation();
			setState(1547);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1543);
					match(T__2);
					setState(1544);
					relation();
					}
					} 
				}
				setState(1549);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			}
			setState(1553);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,194,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1550);
					lateralView();
					}
					} 
				}
				setState(1555);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,194,_ctx);
			}
			setState(1557);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,195,_ctx) ) {
			case 1:
				{
				setState(1556);
				pivotClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAggregation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_aggregation);
		int _la;
		try {
			int _alt;
			setState(1603);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,200,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1559);
				match(GROUP);
				setState(1560);
				match(BY);
				setState(1561);
				((AggregationContext)_localctx).expression = expression();
				((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
				setState(1566);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,196,_ctx);
				while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1562);
						match(T__2);
						setState(1563);
						((AggregationContext)_localctx).expression = expression();
						((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
						}
						} 
					}
					setState(1568);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,196,_ctx);
				}
				setState(1586);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,198,_ctx) ) {
				case 1:
					{
					setState(1569);
					match(WITH);
					setState(1570);
					((AggregationContext)_localctx).kind = match(ROLLUP);
					}
					break;
				case 2:
					{
					setState(1571);
					match(WITH);
					setState(1572);
					((AggregationContext)_localctx).kind = match(CUBE);
					}
					break;
				case 3:
					{
					setState(1573);
					((AggregationContext)_localctx).kind = match(GROUPING);
					setState(1574);
					match(SETS);
					setState(1575);
					match(T__0);
					setState(1576);
					groupingSet();
					setState(1581);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1577);
						match(T__2);
						setState(1578);
						groupingSet();
						}
						}
						setState(1583);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1584);
					match(T__1);
					}
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1588);
				match(GROUP);
				setState(1589);
				match(BY);
				setState(1590);
				((AggregationContext)_localctx).kind = match(GROUPING);
				setState(1591);
				match(SETS);
				setState(1592);
				match(T__0);
				setState(1593);
				groupingSet();
				setState(1598);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1594);
					match(T__2);
					setState(1595);
					groupingSet();
					}
					}
					setState(1600);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1601);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_groupingSet);
		int _la;
		try {
			setState(1618);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1605);
				match(T__0);
				setState(1614);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
				case 1:
					{
					setState(1606);
					expression();
					setState(1611);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1607);
						match(T__2);
						setState(1608);
						expression();
						}
						}
						setState(1613);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(1616);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1617);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotClauseContext extends ParserRuleContext {
		public NamedExpressionSeqContext aggregates;
		public PivotValueContext pivotValue;
		public List<PivotValueContext> pivotValues = new ArrayList<PivotValueContext>();
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public PivotColumnContext pivotColumn() {
			return getRuleContext(PivotColumnContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<PivotValueContext> pivotValue() {
			return getRuleContexts(PivotValueContext.class);
		}
		public PivotValueContext pivotValue(int i) {
			return getRuleContext(PivotValueContext.class,i);
		}
		public PivotClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPivotClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotClauseContext pivotClause() throws RecognitionException {
		PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_pivotClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1620);
			match(PIVOT);
			setState(1621);
			match(T__0);
			setState(1622);
			((PivotClauseContext)_localctx).aggregates = namedExpressionSeq();
			setState(1623);
			match(FOR);
			setState(1624);
			pivotColumn();
			setState(1625);
			match(IN);
			setState(1626);
			match(T__0);
			setState(1627);
			((PivotClauseContext)_localctx).pivotValue = pivotValue();
			((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
			setState(1632);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1628);
				match(T__2);
				setState(1629);
				((PivotClauseContext)_localctx).pivotValue = pivotValue();
				((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
				}
				}
				setState(1634);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1635);
			match(T__1);
			setState(1636);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotColumnContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> identifiers = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public PivotColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPivotColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotColumnContext pivotColumn() throws RecognitionException {
		PivotColumnContext _localctx = new PivotColumnContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_pivotColumn);
		int _la;
		try {
			setState(1650);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1638);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1639);
				match(T__0);
				setState(1640);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				setState(1645);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1641);
					match(T__2);
					setState(1642);
					((PivotColumnContext)_localctx).identifier = identifier();
					((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
					}
					}
					setState(1647);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1648);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotValueContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public PivotValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPivotValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotValueContext pivotValue() throws RecognitionException {
		PivotValueContext _localctx = new PivotValueContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_pivotValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1652);
			expression();
			setState(1657);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
			case 1:
				{
				setState(1654);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
				case 1:
					{
					setState(1653);
					match(AS);
					}
					break;
				}
				setState(1656);
				identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateralView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateralView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1659);
			match(LATERAL);
			setState(1660);
			match(VIEW);
			setState(1662);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
			case 1:
				{
				setState(1661);
				match(OUTER);
				}
				break;
			}
			setState(1664);
			qualifiedName();
			setState(1665);
			match(T__0);
			setState(1674);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
			case 1:
				{
				setState(1666);
				expression();
				setState(1671);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1667);
					match(T__2);
					setState(1668);
					expression();
					}
					}
					setState(1673);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(1676);
			match(T__1);
			setState(1677);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1689);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
			case 1:
				{
				setState(1679);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
				case 1:
					{
					setState(1678);
					match(AS);
					}
					break;
				}
				setState(1681);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1686);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,213,_ctx);
				while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1682);
						match(T__2);
						setState(1683);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1688);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,213,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1691);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1693);
			relationPrimary();
			setState(1697);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,215,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1694);
					joinRelation();
					}
					} 
				}
				setState(1699);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,215,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_joinRelation);
		try {
			setState(1711);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ANTI:
			case CROSS:
			case FULL:
			case INNER:
			case JOIN:
			case LEFT:
			case RIGHT:
			case SEMI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1700);
				joinType();
				}
				setState(1701);
				match(JOIN);
				setState(1702);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(1704);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
				case 1:
					{
					setState(1703);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1706);
				match(NATURAL);
				setState(1707);
				joinType();
				setState(1708);
				match(JOIN);
				setState(1709);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_joinType);
		int _la;
		try {
			setState(1737);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,224,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1714);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1713);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1716);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1717);
				match(LEFT);
				setState(1719);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1718);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1722);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1721);
					match(LEFT);
					}
				}

				setState(1724);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1725);
				match(RIGHT);
				setState(1727);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1726);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1729);
				match(FULL);
				setState(1731);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1730);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1734);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1733);
					match(LEFT);
					}
				}

				setState(1736);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_joinCriteria);
		try {
			setState(1743);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1739);
				match(ON);
				setState(1740);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1741);
				match(USING);
				setState(1742);
				identifierList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleContext extends ParserRuleContext {
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleMethodContext sampleMethod() {
			return getRuleContext(SampleMethodContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_sample);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1745);
			match(TABLESAMPLE);
			setState(1746);
			match(T__0);
			setState(1748);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
			case 1:
				{
				setState(1747);
				sampleMethod();
				}
				break;
			}
			setState(1750);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleMethodContext extends ParserRuleContext {
		public SampleMethodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleMethod; }
	 
		public SampleMethodContext() { }
		public void copyFrom(SampleMethodContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SampleByRowsContext extends SampleMethodContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public SampleByRowsContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByRows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByRows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleByRows(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByPercentileContext extends SampleMethodContext {
		public Token negativeSign;
		public Token percentage;
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SampleByPercentileContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByPercentile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByPercentile(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleByPercentile(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByBucketContext extends SampleMethodContext {
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleByBucketContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByBucket(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByBucket(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleByBucket(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByBytesContext extends SampleMethodContext {
		public ExpressionContext bytes;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampleByBytesContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByBytes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByBytes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleByBytes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleMethodContext sampleMethod() throws RecognitionException {
		SampleMethodContext _localctx = new SampleMethodContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_sampleMethod);
		int _la;
		try {
			setState(1776);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,230,_ctx) ) {
			case 1:
				_localctx = new SampleByPercentileContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1753);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(1752);
					((SampleByPercentileContext)_localctx).negativeSign = match(MINUS);
					}
				}

				setState(1755);
				((SampleByPercentileContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleByPercentileContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1756);
				match(PERCENTLIT);
				}
				break;
			case 2:
				_localctx = new SampleByRowsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1757);
				expression();
				setState(1758);
				match(ROWS);
				}
				break;
			case 3:
				_localctx = new SampleByBucketContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1760);
				((SampleByBucketContext)_localctx).sampleType = match(BUCKET);
				setState(1761);
				((SampleByBucketContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(1762);
				match(OUT);
				setState(1763);
				match(OF);
				setState(1764);
				((SampleByBucketContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(1773);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1765);
					match(ON);
					setState(1771);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
					case 1:
						{
						setState(1766);
						identifier();
						}
						break;
					case 2:
						{
						setState(1767);
						qualifiedName();
						setState(1768);
						match(T__0);
						setState(1769);
						match(T__1);
						}
						break;
					}
					}
				}

				}
				break;
			case 4:
				_localctx = new SampleByBytesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1775);
				((SampleByBytesContext)_localctx).bytes = expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1778);
			match(T__0);
			setState(1779);
			identifierSeq();
			setState(1780);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierSeqContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1782);
			identifier();
			setState(1787);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,231,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1783);
					match(T__2);
					setState(1784);
					identifier();
					}
					} 
				}
				setState(1789);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,231,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1790);
			match(T__0);
			setState(1791);
			orderedIdentifier();
			setState(1796);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1792);
				match(T__2);
				setState(1793);
				orderedIdentifier();
				}
				}
				setState(1798);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1799);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierContext extends ParserRuleContext {
		public Token ordering;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1801);
			identifier();
			setState(1803);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1802);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierCommentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierCommentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1805);
			match(T__0);
			setState(1806);
			identifierComment();
			setState(1811);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1807);
				match(T__2);
				setState(1808);
				identifierComment();
				}
				}
				setState(1813);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1814);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1816);
			identifier();
			setState(1819);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1817);
				match(COMMENT);
				setState(1818);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public FunctionTableContext functionTable() {
			return getRuleContext(FunctionTableContext.class,0);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableValuedFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableValuedFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_relationPrimary);
		try {
			setState(1845);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1821);
				tableIdentifier();
				setState(1823);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
				case 1:
					{
					setState(1822);
					sample();
					}
					break;
				}
				setState(1825);
				tableAlias();
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1827);
				match(T__0);
				setState(1828);
				queryNoWith();
				setState(1829);
				match(T__1);
				setState(1831);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
				case 1:
					{
					setState(1830);
					sample();
					}
					break;
				}
				setState(1833);
				tableAlias();
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1835);
				match(T__0);
				setState(1836);
				relation();
				setState(1837);
				match(T__1);
				setState(1839);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
				case 1:
					{
					setState(1838);
					sample();
					}
					break;
				}
				setState(1841);
				tableAlias();
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1843);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1844);
				functionTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1847);
			match(VALUES);
			setState(1848);
			expression();
			setState(1853);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,240,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1849);
					match(T__2);
					setState(1850);
					expression();
					}
					} 
				}
				setState(1855);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,240,_ctx);
			}
			setState(1856);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionTableContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionTableContext functionTable() throws RecognitionException {
		FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_functionTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1858);
			identifier();
			setState(1859);
			match(T__0);
			setState(1868);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,242,_ctx) ) {
			case 1:
				{
				setState(1860);
				expression();
				setState(1865);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1861);
					match(T__2);
					setState(1862);
					expression();
					}
					}
					setState(1867);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(1870);
			match(T__1);
			setState(1871);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableAliasContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TableAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableAliasContext tableAlias() throws RecognitionException {
		TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_tableAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1880);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,245,_ctx) ) {
			case 1:
				{
				setState(1874);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
				case 1:
					{
					setState(1873);
					match(AS);
					}
					break;
				}
				setState(1876);
				strictIdentifier();
				setState(1878);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
				case 1:
					{
					setState(1877);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatSerde(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatSerde(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatDelimited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatDelimited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_rowFormat);
		try {
			setState(1931);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,253,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1882);
				match(ROW);
				setState(1883);
				match(FORMAT);
				setState(1884);
				match(SERDE);
				setState(1885);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(1889);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
				case 1:
					{
					setState(1886);
					match(WITH);
					setState(1887);
					match(SERDEPROPERTIES);
					setState(1888);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1891);
				match(ROW);
				setState(1892);
				match(FORMAT);
				setState(1893);
				match(DELIMITED);
				setState(1903);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
				case 1:
					{
					setState(1894);
					match(FIELDS);
					setState(1895);
					match(TERMINATED);
					setState(1896);
					match(BY);
					setState(1897);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(1901);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
					case 1:
						{
						setState(1898);
						match(ESCAPED);
						setState(1899);
						match(BY);
						setState(1900);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(1910);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,249,_ctx) ) {
				case 1:
					{
					setState(1905);
					match(COLLECTION);
					setState(1906);
					match(ITEMS);
					setState(1907);
					match(TERMINATED);
					setState(1908);
					match(BY);
					setState(1909);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1917);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,250,_ctx) ) {
				case 1:
					{
					setState(1912);
					match(MAP);
					setState(1913);
					match(KEYS);
					setState(1914);
					match(TERMINATED);
					setState(1915);
					match(BY);
					setState(1916);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1923);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
				case 1:
					{
					setState(1919);
					match(LINES);
					setState(1920);
					match(TERMINATED);
					setState(1921);
					match(BY);
					setState(1922);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(1929);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,252,_ctx) ) {
				case 1:
					{
					setState(1925);
					match(NULL);
					setState(1926);
					match(DEFINED);
					setState(1927);
					match(AS);
					setState(1928);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultipartIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> parts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
		MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_multipartIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1933);
			((MultipartIdentifierContext)_localctx).identifier = identifier();
			((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
			setState(1938);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1934);
				match(T__3);
				setState(1935);
				((MultipartIdentifierContext)_localctx).identifier = identifier();
				((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
				}
				}
				setState(1940);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext table;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1944);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
			case 1:
				{
				setState(1941);
				((TableIdentifierContext)_localctx).db = identifier();
				setState(1942);
				match(T__3);
				}
				break;
			}
			setState(1946);
			((TableIdentifierContext)_localctx).table = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext function;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
		FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_functionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1951);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,256,_ctx) ) {
			case 1:
				{
				setState(1948);
				((FunctionIdentifierContext)_localctx).db = identifier();
				setState(1949);
				match(T__3);
				}
				break;
			}
			setState(1953);
			((FunctionIdentifierContext)_localctx).function = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1955);
			expression();
			setState(1963);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
			case 1:
				{
				setState(1957);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,257,_ctx) ) {
				case 1:
					{
					setState(1956);
					match(AS);
					}
					break;
				}
				setState(1961);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,258,_ctx) ) {
				case 1:
					{
					setState(1959);
					identifier();
					}
					break;
				case 2:
					{
					setState(1960);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1965);
			namedExpression();
			setState(1970);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,260,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1966);
					match(T__2);
					setState(1967);
					namedExpression();
					}
					} 
				}
				setState(1972);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,260,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformListContext extends ParserRuleContext {
		public TransformContext transform;
		public List<TransformContext> transforms = new ArrayList<TransformContext>();
		public List<TransformContext> transform() {
			return getRuleContexts(TransformContext.class);
		}
		public TransformContext transform(int i) {
			return getRuleContext(TransformContext.class,i);
		}
		public TransformListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransformList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformListContext transformList() throws RecognitionException {
		TransformListContext _localctx = new TransformListContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_transformList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1973);
			match(T__0);
			setState(1974);
			((TransformListContext)_localctx).transform = transform();
			((TransformListContext)_localctx).transforms.add(((TransformListContext)_localctx).transform);
			setState(1979);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1975);
				match(T__2);
				setState(1976);
				((TransformListContext)_localctx).transform = transform();
				((TransformListContext)_localctx).transforms.add(((TransformListContext)_localctx).transform);
				}
				}
				setState(1981);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1982);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformContext extends ParserRuleContext {
		public TransformContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform; }
	 
		public TransformContext() { }
		public void copyFrom(TransformContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class IdentityTransformContext extends TransformContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentityTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentityTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentityTransform(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentityTransform(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ApplyTransformContext extends TransformContext {
		public IdentifierContext transformName;
		public TransformArgumentContext transformArgument;
		public List<TransformArgumentContext> argument = new ArrayList<TransformArgumentContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TransformArgumentContext> transformArgument() {
			return getRuleContexts(TransformArgumentContext.class);
		}
		public TransformArgumentContext transformArgument(int i) {
			return getRuleContext(TransformArgumentContext.class,i);
		}
		public ApplyTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterApplyTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitApplyTransform(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitApplyTransform(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformContext transform() throws RecognitionException {
		TransformContext _localctx = new TransformContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_transform);
		int _la;
		try {
			setState(1997);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,263,_ctx) ) {
			case 1:
				_localctx = new IdentityTransformContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1984);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new ApplyTransformContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1985);
				((ApplyTransformContext)_localctx).transformName = identifier();
				setState(1986);
				match(T__0);
				setState(1987);
				((ApplyTransformContext)_localctx).transformArgument = transformArgument();
				((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
				setState(1992);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1988);
					match(T__2);
					setState(1989);
					((ApplyTransformContext)_localctx).transformArgument = transformArgument();
					((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
					}
					}
					setState(1994);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1995);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformArgumentContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TransformArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransformArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformArgumentContext transformArgument() throws RecognitionException {
		TransformArgumentContext _localctx = new TransformArgumentContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_transformArgument);
		try {
			setState(2001);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,264,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1999);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2000);
				constant();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2003);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 158;
		enterRecursionRule(_localctx, 158, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2017);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,266,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2006);
				match(NOT);
				setState(2007);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2008);
				match(EXISTS);
				setState(2009);
				match(T__0);
				setState(2010);
				query();
				setState(2011);
				match(T__1);
				}
				break;
			case 3:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2013);
				valueExpression(0);
				setState(2015);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,265,_ctx) ) {
				case 1:
					{
					setState(2014);
					predicate();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2027);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,268,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2025);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,267,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2019);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2020);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(2021);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2022);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2023);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(2024);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(2029);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,268,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public ValueExpressionContext right;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_predicate);
		int _la;
		try {
			setState(2078);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2031);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2030);
					match(NOT);
					}
				}

				setState(2033);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(2034);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(2035);
				match(AND);
				setState(2036);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2039);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2038);
					match(NOT);
					}
				}

				setState(2041);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2042);
				match(T__0);
				setState(2043);
				expression();
				setState(2048);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(2044);
					match(T__2);
					setState(2045);
					expression();
					}
					}
					setState(2050);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2051);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2054);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2053);
					match(NOT);
					}
				}

				setState(2056);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2057);
				match(T__0);
				setState(2058);
				query();
				setState(2059);
				match(T__1);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2062);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2061);
					match(NOT);
					}
				}

				setState(2064);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==RLIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2065);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2066);
				match(IS);
				setState(2068);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2067);
					match(NOT);
					}
				}

				setState(2070);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2071);
				match(IS);
				setState(2073);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2072);
					match(NOT);
					}
				}

				setState(2075);
				((PredicateContext)_localctx).kind = match(DISTINCT);
				setState(2076);
				match(FROM);
				setState(2077);
				((PredicateContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 162;
		enterRecursionRule(_localctx, 162, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2084);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,277,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2081);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2082);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 267)) & ~0x3f) == 0 && ((1L << (_la - 267)) & ((1L << (PLUS - 267)) | (1L << (MINUS - 267)) | (1L << (TILDE - 267)))) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2083);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2107);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,279,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2105);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,278,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2086);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2087);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 269)) & ~0x3f) == 0 && ((1L << (_la - 269)) & ((1L << (ASTERISK - 269)) | (1L << (SLASH - 269)) | (1L << (PERCENT - 269)) | (1L << (DIV - 269)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2088);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2089);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2090);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 267)) & ~0x3f) == 0 && ((1L << (_la - 267)) & ((1L << (PLUS - 267)) | (1L << (MINUS - 267)) | (1L << (CONCAT_PIPE - 267)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2091);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2092);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2093);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(2094);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2095);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2096);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(2097);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2098);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2099);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(2100);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2101);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2102);
						comparisonOperator();
						setState(2103);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(2109);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,279,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class StructContext extends PrimaryExpressionContext {
		public NamedExpressionContext namedExpression;
		public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public StructContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStruct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStruct(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStruct(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LastContext extends PrimaryExpressionContext {
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public LastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CurrentDatetimeContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public CurrentDatetimeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentDatetime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentDatetime(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentDatetime(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(SqlBaseParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SqlBaseParser.IDENTIFIER, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExtractContext extends PrimaryExpressionContext {
		public IdentifierContext field;
		public ValueExpressionContext source;
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public Token trimOption;
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PositionContext extends PrimaryExpressionContext {
		public ValueExpressionContext substr;
		public ValueExpressionContext str;
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FirstContext extends PrimaryExpressionContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public FirstContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFirst(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFirst(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFirst(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 164;
		enterRecursionRule(_localctx, 164, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2256);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,294,_ctx) ) {
			case 1:
				{
				_localctx = new CurrentDatetimeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2111);
				((CurrentDatetimeContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==CURRENT_DATE || _la==CURRENT_TIMESTAMP) ) {
					((CurrentDatetimeContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2112);
				match(CASE);
				setState(2114); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2113);
					whenClause();
					}
					}
					setState(2116); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2118);
					match(ELSE);
					setState(2119);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2122);
				match(END);
				}
				break;
			case 3:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2124);
				match(CASE);
				setState(2125);
				((SimpleCaseContext)_localctx).value = expression();
				setState(2127); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2126);
					whenClause();
					}
					}
					setState(2129); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2133);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2131);
					match(ELSE);
					setState(2132);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2135);
				match(END);
				}
				break;
			case 4:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2137);
				match(CAST);
				setState(2138);
				match(T__0);
				setState(2139);
				expression();
				setState(2140);
				match(AS);
				setState(2141);
				dataType();
				setState(2142);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new StructContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2144);
				match(STRUCT);
				setState(2145);
				match(T__0);
				setState(2154);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,285,_ctx) ) {
				case 1:
					{
					setState(2146);
					((StructContext)_localctx).namedExpression = namedExpression();
					((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
					setState(2151);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2147);
						match(T__2);
						setState(2148);
						((StructContext)_localctx).namedExpression = namedExpression();
						((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
						}
						}
						setState(2153);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2156);
				match(T__1);
				}
				break;
			case 6:
				{
				_localctx = new FirstContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2157);
				match(FIRST);
				setState(2158);
				match(T__0);
				setState(2159);
				expression();
				setState(2162);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2160);
					match(IGNORE);
					setState(2161);
					match(NULLS);
					}
				}

				setState(2164);
				match(T__1);
				}
				break;
			case 7:
				{
				_localctx = new LastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2166);
				match(LAST);
				setState(2167);
				match(T__0);
				setState(2168);
				expression();
				setState(2171);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2169);
					match(IGNORE);
					setState(2170);
					match(NULLS);
					}
				}

				setState(2173);
				match(T__1);
				}
				break;
			case 8:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2175);
				match(POSITION);
				setState(2176);
				match(T__0);
				setState(2177);
				((PositionContext)_localctx).substr = valueExpression(0);
				setState(2178);
				match(IN);
				setState(2179);
				((PositionContext)_localctx).str = valueExpression(0);
				setState(2180);
				match(T__1);
				}
				break;
			case 9:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2182);
				constant();
				}
				break;
			case 10:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2183);
				match(ASTERISK);
				}
				break;
			case 11:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2184);
				qualifiedName();
				setState(2185);
				match(T__3);
				setState(2186);
				match(ASTERISK);
				}
				break;
			case 12:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2188);
				match(T__0);
				setState(2189);
				namedExpression();
				setState(2192); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2190);
					match(T__2);
					setState(2191);
					namedExpression();
					}
					}
					setState(2194); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(2196);
				match(T__1);
				}
				break;
			case 13:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2198);
				match(T__0);
				setState(2199);
				query();
				setState(2200);
				match(T__1);
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2202);
				qualifiedName();
				setState(2203);
				match(T__0);
				setState(2215);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,291,_ctx) ) {
				case 1:
					{
					setState(2205);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,289,_ctx) ) {
					case 1:
						{
						setState(2204);
						setQuantifier();
						}
						break;
					}
					setState(2207);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(2212);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2208);
						match(T__2);
						setState(2209);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(2214);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2217);
				match(T__1);
				setState(2220);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,292,_ctx) ) {
				case 1:
					{
					setState(2218);
					match(OVER);
					setState(2219);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 15:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2222);
				qualifiedName();
				setState(2223);
				match(T__0);
				setState(2224);
				((FunctionCallContext)_localctx).trimOption = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
					((FunctionCallContext)_localctx).trimOption = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2225);
				((FunctionCallContext)_localctx).expression = expression();
				((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
				setState(2226);
				match(FROM);
				setState(2227);
				((FunctionCallContext)_localctx).expression = expression();
				((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
				setState(2228);
				match(T__1);
				}
				break;
			case 16:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2230);
				match(IDENTIFIER);
				setState(2231);
				match(T__6);
				setState(2232);
				expression();
				}
				break;
			case 17:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2233);
				match(T__0);
				setState(2234);
				match(IDENTIFIER);
				setState(2237); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2235);
					match(T__2);
					setState(2236);
					match(IDENTIFIER);
					}
					}
					setState(2239); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(2241);
				match(T__1);
				setState(2242);
				match(T__6);
				setState(2243);
				expression();
				}
				break;
			case 18:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2244);
				identifier();
				}
				break;
			case 19:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2245);
				match(T__0);
				setState(2246);
				expression();
				setState(2247);
				match(T__1);
				}
				break;
			case 20:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2249);
				match(EXTRACT);
				setState(2250);
				match(T__0);
				setState(2251);
				((ExtractContext)_localctx).field = identifier();
				setState(2252);
				match(FROM);
				setState(2253);
				((ExtractContext)_localctx).source = valueExpression(0);
				setState(2254);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2268);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,296,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2266);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,295,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2258);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2259);
						match(T__7);
						setState(2260);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2261);
						match(T__8);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2263);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2264);
						match(T__3);
						setState(2265);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2270);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,296,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_constant);
		try {
			int _alt;
			setState(2283);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,298,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2271);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2272);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2273);
				identifier();
				setState(2274);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2276);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2277);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2279); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2278);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2281); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,297,_ctx);
				} while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2285);
			_la = _input.LA(1);
			if ( !(((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & ((1L << (EQ - 259)) | (1L << (NSEQ - 259)) | (1L << (NEQ - 259)) | (1L << (NEQJ - 259)) | (1L << (LT - 259)) | (1L << (LTE - 259)) | (1L << (GT - 259)) | (1L << (GTE - 259)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2287);
			_la = _input.LA(1);
			if ( !(((((_la - 267)) & ~0x3f) == 0 && ((1L << (_la - 267)) & ((1L << (PLUS - 267)) | (1L << (MINUS - 267)) | (1L << (ASTERISK - 267)) | (1L << (SLASH - 267)) | (1L << (PERCENT - 267)) | (1L << (DIV - 267)) | (1L << (TILDE - 267)) | (1L << (AMPERSAND - 267)) | (1L << (PIPE - 267)) | (1L << (CONCAT_PIPE - 267)) | (1L << (HAT - 267)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2289);
			_la = _input.LA(1);
			if ( !(_la==AND || ((((_la - 112)) & ~0x3f) == 0 && ((1L << (_la - 112)) & ((1L << (IN - 112)) | (1L << (NOT - 112)) | (1L << (OR - 112)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2291);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_interval);
		int _la;
		try {
			int _alt;
			setState(2310);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,302,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2293);
				if (!(ansi)) throw new FailedPredicateException(this, "ansi");
				setState(2295);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTERVAL) {
					{
					setState(2294);
					match(INTERVAL);
					}
				}

				setState(2298); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2297);
						intervalField();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2300); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,300,_ctx);
				} while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2302);
				if (!(!ansi)) throw new FailedPredicateException(this, "!ansi");
				setState(2303);
				match(INTERVAL);
				setState(2307);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,301,_ctx);
				while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2304);
						intervalField();
						}
						} 
					}
					setState(2309);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,301,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalFieldContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IntervalUnitContext unit;
		public IntervalUnitContext to;
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IntervalUnitContext> intervalUnit() {
			return getRuleContexts(IntervalUnitContext.class);
		}
		public IntervalUnitContext intervalUnit(int i) {
			return getRuleContext(IntervalUnitContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_intervalField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2312);
			((IntervalFieldContext)_localctx).value = intervalValue();
			setState(2313);
			((IntervalFieldContext)_localctx).unit = intervalUnit();
			setState(2316);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,303,_ctx) ) {
			case 1:
				{
				setState(2314);
				match(TO);
				setState(2315);
				((IntervalFieldContext)_localctx).to = intervalUnit();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_intervalValue);
		int _la;
		try {
			setState(2323);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2319);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(2318);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2321);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2322);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalUnitContext extends ParserRuleContext {
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DAYS() { return getToken(SqlBaseParser.DAYS, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode HOURS() { return getToken(SqlBaseParser.HOURS, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public TerminalNode MICROSECONDS() { return getToken(SqlBaseParser.MICROSECONDS, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MILLISECONDS() { return getToken(SqlBaseParser.MILLISECONDS, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MINUTES() { return getToken(SqlBaseParser.MINUTES, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode MONTHS() { return getToken(SqlBaseParser.MONTHS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SECONDS() { return getToken(SqlBaseParser.SECONDS, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode WEEKS() { return getToken(SqlBaseParser.WEEKS, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode YEARS() { return getToken(SqlBaseParser.YEARS, 0); }
		public IntervalUnitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalUnit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalUnit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalUnit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalUnit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalUnitContext intervalUnit() throws RecognitionException {
		IntervalUnitContext _localctx = new IntervalUnitContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_intervalUnit);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2325);
			_la = _input.LA(1);
			if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (DAY - 63)) | (1L << (DAYS - 63)) | (1L << (HOUR - 63)) | (1L << (HOURS - 63)))) != 0) || ((((_la - 143)) & ~0x3f) == 0 && ((1L << (_la - 143)) & ((1L << (MICROSECOND - 143)) | (1L << (MICROSECONDS - 143)) | (1L << (MILLISECOND - 143)) | (1L << (MILLISECONDS - 143)) | (1L << (MINUTE - 143)) | (1L << (MINUTES - 143)) | (1L << (MONTH - 143)) | (1L << (MONTHS - 143)) | (1L << (SECOND - 143)) | (1L << (SECONDS - 143)))) != 0) || ((((_la - 251)) & ~0x3f) == 0 && ((1L << (_la - 251)) & ((1L << (WEEK - 251)) | (1L << (WEEKS - 251)) | (1L << (YEAR - 251)) | (1L << (YEARS - 251)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColPositionContext extends ParserRuleContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColPosition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_colPosition);
		try {
			setState(2330);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(2327);
				match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2328);
				match(AFTER);
				setState(2329);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_dataType);
		int _la;
		try {
			setState(2366);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,311,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2332);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(2333);
				match(LT);
				setState(2334);
				dataType();
				setState(2335);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2337);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(2338);
				match(LT);
				setState(2339);
				dataType();
				setState(2340);
				match(T__2);
				setState(2341);
				dataType();
				setState(2342);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2344);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(2351);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(2345);
					match(LT);
					setState(2347);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,307,_ctx) ) {
					case 1:
						{
						setState(2346);
						complexColTypeList();
						}
						break;
					}
					setState(2349);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(2350);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2353);
				identifier();
				setState(2364);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,310,_ctx) ) {
				case 1:
					{
					setState(2354);
					match(T__0);
					setState(2355);
					match(INTEGER_VALUE);
					setState(2360);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2356);
						match(T__2);
						setState(2357);
						match(INTEGER_VALUE);
						}
						}
						setState(2362);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2363);
					match(T__1);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2368);
			colType();
			setState(2373);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,312,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2369);
					match(T__2);
					setState(2370);
					colType();
					}
					} 
				}
				setState(2375);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,312,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2376);
			identifier();
			setState(2377);
			dataType();
			setState(2380);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,313,_ctx) ) {
			case 1:
				{
				setState(2378);
				match(COMMENT);
				setState(2379);
				match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2382);
			complexColType();
			setState(2387);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2383);
				match(T__2);
				setState(2384);
				complexColType();
				}
				}
				setState(2389);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2390);
			identifier();
			setState(2391);
			match(T__9);
			setState(2392);
			dataType();
			setState(2395);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2393);
				match(COMMENT);
				setState(2394);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2397);
			match(WHEN);
			setState(2398);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2399);
			match(THEN);
			setState(2400);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowsContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windows; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindows(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowsContext windows() throws RecognitionException {
		WindowsContext _localctx = new WindowsContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_windows);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2402);
			match(WINDOW);
			setState(2403);
			namedWindow();
			setState(2408);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,316,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2404);
					match(T__2);
					setState(2405);
					namedWindow();
					}
					} 
				}
				setState(2410);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,316,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedWindowContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2411);
			identifier();
			setState(2412);
			match(AS);
			setState(2413);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class WindowRefContext extends WindowSpecContext {
		public IdentifierContext name;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowRef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_windowSpec);
		int _la;
		try {
			setState(2461);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,324,_ctx) ) {
			case 1:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2415);
				((WindowRefContext)_localctx).name = identifier();
				}
				break;
			case 2:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2416);
				match(T__0);
				setState(2417);
				((WindowRefContext)_localctx).name = identifier();
				setState(2418);
				match(T__1);
				}
				break;
			case 3:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2420);
				match(T__0);
				setState(2455);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2421);
					match(CLUSTER);
					setState(2422);
					match(BY);
					setState(2423);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2428);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2424);
						match(T__2);
						setState(2425);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2430);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__1:
				case DISTRIBUTE:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
					{
					setState(2441);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==DISTRIBUTE || _la==PARTITION) {
						{
						setState(2431);
						_la = _input.LA(1);
						if ( !(_la==DISTRIBUTE || _la==PARTITION) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2432);
						match(BY);
						setState(2433);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2438);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2434);
							match(T__2);
							setState(2435);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2440);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2453);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2443);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2444);
						match(BY);
						setState(2445);
						sortItem();
						setState(2450);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2446);
							match(T__2);
							setState(2447);
							sortItem();
							}
							}
							setState(2452);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2458);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2457);
					windowFrame();
					}
				}

				setState(2460);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_windowFrame);
		try {
			setState(2479);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,325,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2463);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2464);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2465);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2466);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2467);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2468);
				match(BETWEEN);
				setState(2469);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2470);
				match(AND);
				setState(2471);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2473);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2474);
				match(BETWEEN);
				setState(2475);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2476);
				match(AND);
				setState(2477);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFrameBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_frameBound);
		int _la;
		try {
			setState(2488);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,326,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2481);
				match(UNBOUNDED);
				setState(2482);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2483);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2484);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2485);
				expression();
				setState(2486);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2490);
			identifier();
			setState(2495);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,327,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2491);
					match(T__3);
					setState(2492);
					identifier();
					}
					} 
				}
				setState(2497);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,327,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public StrictNonReservedContext strictNonReserved() {
			return getRuleContext(StrictNonReservedContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_identifier);
		try {
			setState(2501);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,328,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2498);
				strictIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2499);
				if (!(!ansi)) throw new FailedPredicateException(this, "!ansi");
				setState(2500);
				strictNonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public AnsiNonReservedContext ansiNonReserved() {
			return getRuleContext(AnsiNonReservedContext.class,0);
		}
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_strictIdentifier);
		try {
			setState(2509);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,329,_ctx) ) {
			case 1:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2503);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2504);
				quotedIdentifier();
				}
				break;
			case 3:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2505);
				if (!(ansi)) throw new FailedPredicateException(this, "ansi");
				setState(2506);
				ansiNonReserved();
				}
				break;
			case 4:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2507);
				if (!(!ansi)) throw new FailedPredicateException(this, "!ansi");
				setState(2508);
				nonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2511);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_number);
		int _la;
		try {
			setState(2541);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,337,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2514);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2513);
					match(MINUS);
					}
				}

				setState(2516);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2518);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2517);
					match(MINUS);
					}
				}

				setState(2520);
				match(INTEGER_VALUE);
				}
				break;
			case 3:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2522);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2521);
					match(MINUS);
					}
				}

				setState(2524);
				match(BIGINT_LITERAL);
				}
				break;
			case 4:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2526);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2525);
					match(MINUS);
					}
				}

				setState(2528);
				match(SMALLINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2530);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2529);
					match(MINUS);
					}
				}

				setState(2532);
				match(TINYINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2534);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2533);
					match(MINUS);
					}
				}

				setState(2536);
				match(DOUBLE_LITERAL);
				}
				break;
			case 7:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2538);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2537);
					match(MINUS);
					}
				}

				setState(2540);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnsiNonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DAYS() { return getToken(SqlBaseParser.DAYS, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode HOURS() { return getToken(SqlBaseParser.HOURS, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public TerminalNode MICROSECONDS() { return getToken(SqlBaseParser.MICROSECONDS, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MILLISECONDS() { return getToken(SqlBaseParser.MILLISECONDS, 0); }
		public TerminalNode MINUTES() { return getToken(SqlBaseParser.MINUTES, 0); }
		public TerminalNode MONTHS() { return getToken(SqlBaseParser.MONTHS, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SECONDS() { return getToken(SqlBaseParser.SECONDS, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode WEEKS() { return getToken(SqlBaseParser.WEEKS, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode YEARS() { return getToken(SqlBaseParser.YEARS, 0); }
		public AnsiNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ansiNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnsiNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnsiNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnsiNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnsiNonReservedContext ansiNonReserved() throws RecognitionException {
		AnsiNonReservedContext _localctx = new AnsiNonReservedContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_ansiNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2543);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALTER) | (1L << ANALYZE) | (1L << ARCHIVE) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BETWEEN) | (1L << BUCKET) | (1L << BUCKETS) | (1L << BY) | (1L << CACHE) | (1L << CASCADE) | (1L << CHANGE) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CLUSTERED) | (1L << CODEGEN) | (1L << COLLECTION) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMPACT) | (1L << COMPACTIONS) | (1L << COMPUTE) | (1L << CONCATENATE) | (1L << COST) | (1L << CUBE) | (1L << CURRENT) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (DAYS - 64)) | (1L << (DBPROPERTIES - 64)) | (1L << (DEFINED - 64)) | (1L << (DELETE - 64)) | (1L << (DELIMITED - 64)) | (1L << (DESC - 64)) | (1L << (DESCRIBE - 64)) | (1L << (DFS - 64)) | (1L << (DIRECTORIES - 64)) | (1L << (DIRECTORY - 64)) | (1L << (DISTRIBUTE - 64)) | (1L << (DROP - 64)) | (1L << (ESCAPED - 64)) | (1L << (EXCHANGE - 64)) | (1L << (EXISTS - 64)) | (1L << (EXPLAIN - 64)) | (1L << (EXPORT - 64)) | (1L << (EXTENDED - 64)) | (1L << (EXTERNAL - 64)) | (1L << (EXTRACT - 64)) | (1L << (FIELDS - 64)) | (1L << (FILEFORMAT - 64)) | (1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FORMATTED - 64)) | (1L << (FUNCTION - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GLOBAL - 64)) | (1L << (GROUPING - 64)) | (1L << (HOURS - 64)) | (1L << (IF - 64)) | (1L << (IGNORE - 64)) | (1L << (IMPORT - 64)) | (1L << (INDEX - 64)) | (1L << (INDEXES - 64)) | (1L << (INPATH - 64)) | (1L << (INPUTFORMAT - 64)) | (1L << (INSERT - 64)) | (1L << (INTERVAL - 64)) | (1L << (ITEMS - 64)) | (1L << (KEYS - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (LAZY - 128)) | (1L << (LIKE - 128)) | (1L << (LIMIT - 128)) | (1L << (LINES - 128)) | (1L << (LIST - 128)) | (1L << (LOAD - 128)) | (1L << (LOCAL - 128)) | (1L << (LOCATION - 128)) | (1L << (LOCK - 128)) | (1L << (LOCKS - 128)) | (1L << (LOGICAL - 128)) | (1L << (MACRO - 128)) | (1L << (MAP - 128)) | (1L << (MICROSECOND - 128)) | (1L << (MICROSECONDS - 128)) | (1L << (MILLISECOND - 128)) | (1L << (MILLISECONDS - 128)) | (1L << (MINUTES - 128)) | (1L << (MONTHS - 128)) | (1L << (MSCK - 128)) | (1L << (NO - 128)) | (1L << (NULLS - 128)) | (1L << (OF - 128)) | (1L << (OPTION - 128)) | (1L << (OPTIONS - 128)) | (1L << (OUT - 128)) | (1L << (OUTPUTFORMAT - 128)) | (1L << (OVER - 128)) | (1L << (OVERWRITE - 128)) | (1L << (PARTITION - 128)) | (1L << (PARTITIONED - 128)) | (1L << (PARTITIONS - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (PIVOT - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRINCIPALS - 128)) | (1L << (PURGE - 128)) | (1L << (QUERY - 128)) | (1L << (RANGE - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (RECOVER - 128)) | (1L << (REDUCE - 128)) | (1L << (REFRESH - 128)) | (1L << (RENAME - 128)) | (1L << (REPAIR - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (RESTRICT - 192)) | (1L << (REVOKE - 192)) | (1L << (RLIKE - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (ROLLBACK - 192)) | (1L << (ROLLUP - 192)) | (1L << (ROW - 192)) | (1L << (ROWS - 192)) | (1L << (SCHEMA - 192)) | (1L << (SECONDS - 192)) | (1L << (SEPARATED - 192)) | (1L << (SERDE - 192)) | (1L << (SERDEPROPERTIES - 192)) | (1L << (SET - 192)) | (1L << (SETS - 192)) | (1L << (SHOW - 192)) | (1L << (SKEWED - 192)) | (1L << (SORT - 192)) | (1L << (SORTED - 192)) | (1L << (START - 192)) | (1L << (STATISTICS - 192)) | (1L << (STORED - 192)) | (1L << (STRATIFY - 192)) | (1L << (STRUCT - 192)) | (1L << (TABLES - 192)) | (1L << (TABLESAMPLE - 192)) | (1L << (TBLPROPERTIES - 192)) | (1L << (TEMPORARY - 192)) | (1L << (TERMINATED - 192)) | (1L << (TOUCH - 192)) | (1L << (TRANSACTION - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (TRANSFORM - 192)) | (1L << (TRUE - 192)) | (1L << (TRUNCATE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (UNBOUNDED - 192)) | (1L << (UNCACHE - 192)) | (1L << (UNLOCK - 192)) | (1L << (UNSET - 192)) | (1L << (USE - 192)) | (1L << (VALUES - 192)) | (1L << (VIEW - 192)) | (1L << (WEEK - 192)) | (1L << (WEEKS - 192)) | (1L << (WINDOW - 192)))) != 0) || _la==YEARS || _la==DIV) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictNonReservedContext extends ParserRuleContext {
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public StrictNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStrictNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStrictNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStrictNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictNonReservedContext strictNonReserved() throws RecognitionException {
		StrictNonReservedContext _localctx = new StrictNonReservedContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_strictNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2545);
			_la = _input.LA(1);
			if ( !(((((_la - 17)) & ~0x3f) == 0 && ((1L << (_la - 17)) & ((1L << (ANTI - 17)) | (1L << (CROSS - 17)) | (1L << (EXCEPT - 17)))) != 0) || ((((_la - 99)) & ~0x3f) == 0 && ((1L << (_la - 99)) & ((1L << (FULL - 99)) | (1L << (INNER - 99)) | (1L << (INTERSECT - 99)) | (1L << (JOIN - 99)) | (1L << (LEFT - 99)) | (1L << (NATURAL - 99)) | (1L << (ON - 99)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (RIGHT - 194)) | (1L << (SEMI - 194)) | (1L << (SETMINUS - 194)) | (1L << (UNION - 194)) | (1L << (USING - 194)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CHECK() { return getToken(SqlBaseParser.CHECK, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLATE() { return getToken(SqlBaseParser.COLLATE, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CONSTRAINT() { return getToken(SqlBaseParser.CONSTRAINT, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DAYS() { return getToken(SqlBaseParser.DAYS, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode FOREIGN() { return getToken(SqlBaseParser.FOREIGN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode HOURS() { return getToken(SqlBaseParser.HOURS, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public TerminalNode MICROSECONDS() { return getToken(SqlBaseParser.MICROSECONDS, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MILLISECONDS() { return getToken(SqlBaseParser.MILLISECONDS, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MINUTES() { return getToken(SqlBaseParser.MINUTES, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode MONTHS() { return getToken(SqlBaseParser.MONTHS, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERLAPS() { return getToken(SqlBaseParser.OVERLAPS, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRIMARY() { return getToken(SqlBaseParser.PRIMARY, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFERENCES() { return getToken(SqlBaseParser.REFERENCES, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SECONDS() { return getToken(SqlBaseParser.SECONDS, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SESSION_USER() { return getToken(SqlBaseParser.SESSION_USER, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode WEEKS() { return getToken(SqlBaseParser.WEEKS, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode YEARS() { return getToken(SqlBaseParser.YEARS, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2547);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALL) | (1L << ALTER) | (1L << ANALYZE) | (1L << AND) | (1L << ANY) | (1L << ARCHIVE) | (1L << ARRAY) | (1L << AS) | (1L << ASC) | (1L << AT) | (1L << AUTHORIZATION) | (1L << BETWEEN) | (1L << BOTH) | (1L << BUCKET) | (1L << BUCKETS) | (1L << BY) | (1L << CACHE) | (1L << CASCADE) | (1L << CASE) | (1L << CAST) | (1L << CHANGE) | (1L << CHECK) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CLUSTERED) | (1L << CODEGEN) | (1L << COLLATE) | (1L << COLLECTION) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMPACT) | (1L << COMPACTIONS) | (1L << COMPUTE) | (1L << CONCATENATE) | (1L << CONSTRAINT) | (1L << COST) | (1L << CREATE) | (1L << CUBE) | (1L << CURRENT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << CURRENT_USER) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES) | (1L << DAY))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (DAYS - 64)) | (1L << (DBPROPERTIES - 64)) | (1L << (DEFINED - 64)) | (1L << (DELETE - 64)) | (1L << (DELIMITED - 64)) | (1L << (DESC - 64)) | (1L << (DESCRIBE - 64)) | (1L << (DFS - 64)) | (1L << (DIRECTORIES - 64)) | (1L << (DIRECTORY - 64)) | (1L << (DISTINCT - 64)) | (1L << (DISTRIBUTE - 64)) | (1L << (DROP - 64)) | (1L << (ELSE - 64)) | (1L << (END - 64)) | (1L << (ESCAPED - 64)) | (1L << (EXCHANGE - 64)) | (1L << (EXISTS - 64)) | (1L << (EXPLAIN - 64)) | (1L << (EXPORT - 64)) | (1L << (EXTENDED - 64)) | (1L << (EXTERNAL - 64)) | (1L << (EXTRACT - 64)) | (1L << (FALSE - 64)) | (1L << (FETCH - 64)) | (1L << (FIELDS - 64)) | (1L << (FILEFORMAT - 64)) | (1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FOR - 64)) | (1L << (FOREIGN - 64)) | (1L << (FORMAT - 64)) | (1L << (FORMATTED - 64)) | (1L << (FROM - 64)) | (1L << (FUNCTION - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GLOBAL - 64)) | (1L << (GRANT - 64)) | (1L << (GROUP - 64)) | (1L << (GROUPING - 64)) | (1L << (HAVING - 64)) | (1L << (HOUR - 64)) | (1L << (HOURS - 64)) | (1L << (IF - 64)) | (1L << (IGNORE - 64)) | (1L << (IMPORT - 64)) | (1L << (IN - 64)) | (1L << (INDEX - 64)) | (1L << (INDEXES - 64)) | (1L << (INPATH - 64)) | (1L << (INPUTFORMAT - 64)) | (1L << (INSERT - 64)) | (1L << (INTERVAL - 64)) | (1L << (INTO - 64)) | (1L << (IS - 64)) | (1L << (ITEMS - 64)) | (1L << (KEYS - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (LAZY - 128)) | (1L << (LEADING - 128)) | (1L << (LIKE - 128)) | (1L << (LIMIT - 128)) | (1L << (LINES - 128)) | (1L << (LIST - 128)) | (1L << (LOAD - 128)) | (1L << (LOCAL - 128)) | (1L << (LOCATION - 128)) | (1L << (LOCK - 128)) | (1L << (LOCKS - 128)) | (1L << (LOGICAL - 128)) | (1L << (MACRO - 128)) | (1L << (MAP - 128)) | (1L << (MICROSECOND - 128)) | (1L << (MICROSECONDS - 128)) | (1L << (MILLISECOND - 128)) | (1L << (MILLISECONDS - 128)) | (1L << (MINUTE - 128)) | (1L << (MINUTES - 128)) | (1L << (MONTH - 128)) | (1L << (MONTHS - 128)) | (1L << (MSCK - 128)) | (1L << (NO - 128)) | (1L << (NOT - 128)) | (1L << (NULL - 128)) | (1L << (NULLS - 128)) | (1L << (OF - 128)) | (1L << (ONLY - 128)) | (1L << (OPTION - 128)) | (1L << (OPTIONS - 128)) | (1L << (OR - 128)) | (1L << (ORDER - 128)) | (1L << (OUT - 128)) | (1L << (OUTER - 128)) | (1L << (OUTPUTFORMAT - 128)) | (1L << (OVER - 128)) | (1L << (OVERLAPS - 128)) | (1L << (OVERWRITE - 128)) | (1L << (PARTITION - 128)) | (1L << (PARTITIONED - 128)) | (1L << (PARTITIONS - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (PIVOT - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIMARY - 128)) | (1L << (PRINCIPALS - 128)) | (1L << (PURGE - 128)) | (1L << (QUERY - 128)) | (1L << (RANGE - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (RECOVER - 128)) | (1L << (REDUCE - 128)) | (1L << (REFERENCES - 128)) | (1L << (REFRESH - 128)) | (1L << (RENAME - 128)) | (1L << (REPAIR - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (RESTRICT - 192)) | (1L << (REVOKE - 192)) | (1L << (RLIKE - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (ROLLBACK - 192)) | (1L << (ROLLUP - 192)) | (1L << (ROW - 192)) | (1L << (ROWS - 192)) | (1L << (SCHEMA - 192)) | (1L << (SECOND - 192)) | (1L << (SECONDS - 192)) | (1L << (SELECT - 192)) | (1L << (SEPARATED - 192)) | (1L << (SERDE - 192)) | (1L << (SERDEPROPERTIES - 192)) | (1L << (SESSION_USER - 192)) | (1L << (SET - 192)) | (1L << (SETS - 192)) | (1L << (SHOW - 192)) | (1L << (SKEWED - 192)) | (1L << (SOME - 192)) | (1L << (SORT - 192)) | (1L << (SORTED - 192)) | (1L << (START - 192)) | (1L << (STATISTICS - 192)) | (1L << (STORED - 192)) | (1L << (STRATIFY - 192)) | (1L << (STRUCT - 192)) | (1L << (TABLE - 192)) | (1L << (TABLES - 192)) | (1L << (TABLESAMPLE - 192)) | (1L << (TBLPROPERTIES - 192)) | (1L << (TEMPORARY - 192)) | (1L << (TERMINATED - 192)) | (1L << (THEN - 192)) | (1L << (TO - 192)) | (1L << (TOUCH - 192)) | (1L << (TRAILING - 192)) | (1L << (TRANSACTION - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (TRANSFORM - 192)) | (1L << (TRUE - 192)) | (1L << (TRUNCATE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (UNBOUNDED - 192)) | (1L << (UNCACHE - 192)) | (1L << (UNIQUE - 192)) | (1L << (UNLOCK - 192)) | (1L << (UNSET - 192)) | (1L << (USE - 192)) | (1L << (USER - 192)) | (1L << (VALUES - 192)) | (1L << (VIEW - 192)) | (1L << (WEEK - 192)) | (1L << (WEEKS - 192)) | (1L << (WHEN - 192)) | (1L << (WHERE - 192)) | (1L << (WINDOW - 192)))) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & ((1L << (WITH - 256)) | (1L << (YEAR - 256)) | (1L << (YEARS - 256)) | (1L << (DIV - 256)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 39:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 79:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 81:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 82:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 88:
			return interval_sempred((IntervalContext)_localctx, predIndex);
		case 105:
			return identifier_sempred((IdentifierContext)_localctx, predIndex);
		case 106:
			return strictIdentifier_sempred((StrictIdentifierContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 3);
		case 1:
			return legacy_setops_precedence_enbled;
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return !legacy_setops_precedence_enbled;
		case 4:
			return precpred(_ctx, 1);
		case 5:
			return !legacy_setops_precedence_enbled;
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 8:
			return precpred(_ctx, 6);
		case 9:
			return precpred(_ctx, 5);
		case 10:
			return precpred(_ctx, 4);
		case 11:
			return precpred(_ctx, 3);
		case 12:
			return precpred(_ctx, 2);
		case 13:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 14:
			return precpred(_ctx, 5);
		case 15:
			return precpred(_ctx, 3);
		}
		return true;
	}
	private boolean interval_sempred(IntervalContext _localctx, int predIndex) {
		switch (predIndex) {
		case 16:
			return ansi;
		case 17:
			return !ansi;
		}
		return true;
	}
	private boolean identifier_sempred(IdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 18:
			return !ansi;
		}
		return true;
	}
	private boolean strictIdentifier_sempred(StrictIdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 19:
			return ansi;
		case 20:
			return !ansi;
		}
		return true;
	}

	private static final int _serializedATNSegments = 2;
	private static final String _serializedATNSegment0 =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u0126\u09f8\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4"+
		"\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b\3\b\3\b\3\t\3\t\5\t\u00fa"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0104\n\t\3\t\3\t\3\t\5\t\u0109"+
		"\n\t\3\t\5\t\u010c\n\t\3\t\3\t\3\t\5\t\u0111\n\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\5\t\u011e\n\t\3\t\3\t\5\t\u0122\n\t\3\t\3\t\3\t"+
		"\5\t\u0127\n\t\3\t\5\t\u012a\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0131\n\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u013f\n\t\f\t\16\t\u0142"+
		"\13\t\3\t\5\t\u0145\n\t\3\t\5\t\u0148\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u014f"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u015c\n\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\7\t\u0165\n\t\f\t\16\t\u0168\13\t\3\t\5\t\u016b\n"+
		"\t\3\t\5\t\u016e\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0175\n\t\3\t\3\t\3\t\3\t"+
		"\5\t\u017b\n\t\3\t\3\t\3\t\3\t\5\t\u0181\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\5\t\u018c\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\5\t\u01ac\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01b4\n\t\3\t\3\t\5\t\u01b8"+
		"\n\t\3\t\3\t\3\t\5\t\u01bd\n\t\3\t\3\t\3\t\3\t\5\t\u01c3\n\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\5\t\u01cb\n\t\3\t\3\t\3\t\3\t\5\t\u01d1\n\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01de\n\t\3\t\6\t\u01e1\n\t\r\t\16"+
		"\t\u01e2\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01ec\n\t\3\t\6\t\u01ef\n\t\r"+
		"\t\16\t\u01f0\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\5\t\u0201\n\t\3\t\3\t\3\t\7\t\u0206\n\t\f\t\16\t\u0209\13\t\3\t\5\t\u020c"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0214\n\t\3\t\3\t\3\t\7\t\u0219\n\t\f"+
		"\t\16\t\u021c\13\t\3\t\3\t\3\t\3\t\5\t\u0222\n\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0231\n\t\3\t\3\t\5\t\u0235\n\t\3\t"+
		"\3\t\3\t\3\t\5\t\u023b\n\t\3\t\3\t\3\t\3\t\5\t\u0241\n\t\3\t\5\t\u0244"+
		"\n\t\3\t\5\t\u0247\n\t\3\t\3\t\3\t\3\t\5\t\u024d\n\t\3\t\3\t\5\t\u0251"+
		"\n\t\3\t\3\t\5\t\u0255\n\t\3\t\3\t\3\t\5\t\u025a\n\t\3\t\3\t\5\t\u025e"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0266\n\t\3\t\5\t\u0269\n\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\5\t\u0272\n\t\3\t\3\t\3\t\5\t\u0277\n\t\3\t\3\t\3\t"+
		"\3\t\5\t\u027d\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0284\n\t\3\t\5\t\u0287\n\t"+
		"\3\t\3\t\3\t\3\t\5\t\u028d\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u0296\n"+
		"\t\f\t\16\t\u0299\13\t\5\t\u029b\n\t\3\t\3\t\5\t\u029f\n\t\3\t\3\t\3\t"+
		"\5\t\u02a4\n\t\3\t\3\t\3\t\5\t\u02a9\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u02b0"+
		"\n\t\3\t\5\t\u02b3\n\t\3\t\5\t\u02b6\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u02bd"+
		"\n\t\3\t\3\t\3\t\5\t\u02c2\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02cb\n"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02d3\n\t\3\t\3\t\3\t\3\t\5\t\u02d9\n\t"+
		"\3\t\3\t\5\t\u02dd\n\t\3\t\3\t\5\t\u02e1\n\t\3\t\3\t\5\t\u02e5\n\t\5\t"+
		"\u02e7\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02f0\n\t\3\t\3\t\3\t\3\t\5"+
		"\t\u02f6\n\t\3\t\3\t\3\t\3\t\5\t\u02fc\n\t\3\t\5\t\u02ff\n\t\3\t\3\t\5"+
		"\t\u0303\n\t\3\t\5\t\u0306\n\t\3\t\3\t\5\t\u030a\n\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\7\t\u0313\n\t\f\t\16\t\u0316\13\t\5\t\u0318\n\t\3\t\3\t\5\t"+
		"\u031c\n\t\3\t\3\t\3\t\3\t\5\t\u0322\n\t\3\t\5\t\u0325\n\t\3\t\5\t\u0328"+
		"\n\t\3\t\3\t\3\t\3\t\5\t\u032e\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0336\n"+
		"\t\3\t\3\t\3\t\5\t\u033b\n\t\3\t\3\t\3\t\3\t\5\t\u0341\n\t\3\t\3\t\3\t"+
		"\3\t\5\t\u0347\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u0350\n\t\f\t\16\t"+
		"\u0353\13\t\3\t\3\t\3\t\7\t\u0358\n\t\f\t\16\t\u035b\13\t\3\t\3\t\7\t"+
		"\u035f\n\t\f\t\16\t\u0362\13\t\3\t\3\t\3\t\7\t\u0367\n\t\f\t\16\t\u036a"+
		"\13\t\5\t\u036c\n\t\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u0374\n\n\3\n\3\n\5\n"+
		"\u0378\n\n\3\n\3\n\3\n\3\n\3\n\5\n\u037f\n\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\5\n\u03f3\n\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u03fb\n\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\5\n\u0403\n\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u040c\n"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u0418\n\n\3\13\3\13\5\13"+
		"\u041c\n\13\3\13\5\13\u041f\n\13\3\13\3\13\3\13\3\13\5\13\u0425\n\13\3"+
		"\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u042f\n\f\3\f\3\f\3\f\3\f\3\r\3\r"+
		"\3\r\3\r\3\r\3\r\5\r\u043b\n\r\3\r\3\r\3\r\5\r\u0440\n\r\3\16\3\16\3\16"+
		"\3\17\5\17\u0446\n\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\5\20\u0452\n\20\5\20\u0454\n\20\3\20\3\20\3\20\5\20\u0459\n\20\3\20\3"+
		"\20\5\20\u045d\n\20\3\20\3\20\3\20\5\20\u0462\n\20\3\20\3\20\3\20\5\20"+
		"\u0467\n\20\3\20\5\20\u046a\n\20\3\20\3\20\3\20\5\20\u046f\n\20\3\20\3"+
		"\20\5\20\u0473\n\20\3\20\3\20\3\20\5\20\u0478\n\20\5\20\u047a\n\20\3\21"+
		"\3\21\5\21\u047e\n\21\3\22\3\22\3\22\3\22\3\22\7\22\u0485\n\22\f\22\16"+
		"\22\u0488\13\22\3\22\3\22\3\23\3\23\3\23\5\23\u048f\n\23\3\24\3\24\3\25"+
		"\3\25\3\25\3\25\3\25\5\25\u0498\n\25\3\26\3\26\3\26\7\26\u049d\n\26\f"+
		"\26\16\26\u04a0\13\26\3\27\3\27\3\27\3\27\7\27\u04a6\n\27\f\27\16\27\u04a9"+
		"\13\27\3\30\3\30\5\30\u04ad\n\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3"+
		"\32\3\32\3\32\3\32\7\32\u04ba\n\32\f\32\16\32\u04bd\13\32\3\32\3\32\3"+
		"\33\3\33\5\33\u04c3\n\33\3\33\5\33\u04c6\n\33\3\34\3\34\3\34\7\34\u04cb"+
		"\n\34\f\34\16\34\u04ce\13\34\3\34\5\34\u04d1\n\34\3\35\3\35\3\35\3\35"+
		"\5\35\u04d7\n\35\3\36\3\36\3\36\3\36\7\36\u04dd\n\36\f\36\16\36\u04e0"+
		"\13\36\3\36\3\36\3\37\3\37\3\37\3\37\7\37\u04e8\n\37\f\37\16\37\u04eb"+
		"\13\37\3\37\3\37\3 \3 \3 \3 \3 \3 \5 \u04f5\n \3!\3!\3!\3!\3!\5!\u04fc"+
		"\n!\3\"\3\"\3\"\3\"\5\"\u0502\n\"\3#\3#\3#\3$\3$\3$\3$\3$\3$\6$\u050d"+
		"\n$\r$\16$\u050e\5$\u0511\n$\3%\3%\3%\3%\3%\6%\u0518\n%\r%\16%\u0519\5"+
		"%\u051c\n%\3&\3&\3&\3&\3&\7&\u0523\n&\f&\16&\u0526\13&\5&\u0528\n&\3&"+
		"\3&\3&\3&\3&\7&\u052f\n&\f&\16&\u0532\13&\5&\u0534\n&\3&\3&\3&\3&\3&\7"+
		"&\u053b\n&\f&\16&\u053e\13&\5&\u0540\n&\3&\3&\3&\3&\3&\7&\u0547\n&\f&"+
		"\16&\u054a\13&\5&\u054c\n&\3&\5&\u054f\n&\3&\3&\3&\5&\u0554\n&\5&\u0556"+
		"\n&\3\'\3\'\3\'\3(\3(\3(\3)\3)\3)\3)\3)\3)\3)\5)\u0565\n)\3)\3)\3)\3)"+
		"\3)\5)\u056c\n)\3)\3)\3)\3)\3)\5)\u0573\n)\3)\7)\u0576\n)\f)\16)\u0579"+
		"\13)\3*\3*\3*\3*\3*\3*\3*\3*\5*\u0583\n*\3+\3+\5+\u0587\n+\3+\3+\5+\u058b"+
		"\n+\3,\3,\3,\3,\3,\3,\3,\3,\3,\3,\5,\u0597\n,\3,\5,\u059a\n,\3,\3,\5,"+
		"\u059e\n,\3,\3,\3,\3,\3,\3,\3,\3,\5,\u05a8\n,\3,\3,\5,\u05ac\n,\5,\u05ae"+
		"\n,\3,\5,\u05b1\n,\3,\3,\5,\u05b5\n,\3,\5,\u05b8\n,\3,\3,\5,\u05bc\n,"+
		"\3,\3,\7,\u05c0\n,\f,\16,\u05c3\13,\3,\5,\u05c6\n,\3,\3,\5,\u05ca\n,\3"+
		",\3,\3,\5,\u05cf\n,\3,\5,\u05d2\n,\5,\u05d4\n,\3,\7,\u05d7\n,\f,\16,\u05da"+
		"\13,\3,\3,\5,\u05de\n,\3,\5,\u05e1\n,\3,\3,\5,\u05e5\n,\3,\5,\u05e8\n"+
		",\5,\u05ea\n,\3-\3-\3-\5-\u05ef\n-\3-\7-\u05f2\n-\f-\16-\u05f5\13-\3-"+
		"\3-\3.\3.\3.\3.\3.\3.\7.\u05ff\n.\f.\16.\u0602\13.\3.\3.\5.\u0606\n.\3"+
		"/\3/\3/\3/\7/\u060c\n/\f/\16/\u060f\13/\3/\7/\u0612\n/\f/\16/\u0615\13"+
		"/\3/\5/\u0618\n/\3\60\3\60\3\60\3\60\3\60\7\60\u061f\n\60\f\60\16\60\u0622"+
		"\13\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\7\60\u062e\n"+
		"\60\f\60\16\60\u0631\13\60\3\60\3\60\5\60\u0635\n\60\3\60\3\60\3\60\3"+
		"\60\3\60\3\60\3\60\3\60\7\60\u063f\n\60\f\60\16\60\u0642\13\60\3\60\3"+
		"\60\5\60\u0646\n\60\3\61\3\61\3\61\3\61\7\61\u064c\n\61\f\61\16\61\u064f"+
		"\13\61\5\61\u0651\n\61\3\61\3\61\5\61\u0655\n\61\3\62\3\62\3\62\3\62\3"+
		"\62\3\62\3\62\3\62\3\62\3\62\7\62\u0661\n\62\f\62\16\62\u0664\13\62\3"+
		"\62\3\62\3\62\3\63\3\63\3\63\3\63\3\63\7\63\u066e\n\63\f\63\16\63\u0671"+
		"\13\63\3\63\3\63\5\63\u0675\n\63\3\64\3\64\5\64\u0679\n\64\3\64\5\64\u067c"+
		"\n\64\3\65\3\65\3\65\5\65\u0681\n\65\3\65\3\65\3\65\3\65\3\65\7\65\u0688"+
		"\n\65\f\65\16\65\u068b\13\65\5\65\u068d\n\65\3\65\3\65\3\65\5\65\u0692"+
		"\n\65\3\65\3\65\3\65\7\65\u0697\n\65\f\65\16\65\u069a\13\65\5\65\u069c"+
		"\n\65\3\66\3\66\3\67\3\67\7\67\u06a2\n\67\f\67\16\67\u06a5\13\67\38\3"+
		"8\38\38\58\u06ab\n8\38\38\38\38\38\58\u06b2\n8\39\59\u06b5\n9\39\39\3"+
		"9\59\u06ba\n9\39\59\u06bd\n9\39\39\39\59\u06c2\n9\39\39\59\u06c6\n9\3"+
		"9\59\u06c9\n9\39\59\u06cc\n9\3:\3:\3:\3:\5:\u06d2\n:\3;\3;\3;\5;\u06d7"+
		"\n;\3;\3;\3<\5<\u06dc\n<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<"+
		"\3<\5<\u06ee\n<\5<\u06f0\n<\3<\5<\u06f3\n<\3=\3=\3=\3=\3>\3>\3>\7>\u06fc"+
		"\n>\f>\16>\u06ff\13>\3?\3?\3?\3?\7?\u0705\n?\f?\16?\u0708\13?\3?\3?\3"+
		"@\3@\5@\u070e\n@\3A\3A\3A\3A\7A\u0714\nA\fA\16A\u0717\13A\3A\3A\3B\3B"+
		"\3B\5B\u071e\nB\3C\3C\5C\u0722\nC\3C\3C\3C\3C\3C\3C\5C\u072a\nC\3C\3C"+
		"\3C\3C\3C\3C\5C\u0732\nC\3C\3C\3C\3C\5C\u0738\nC\3D\3D\3D\3D\7D\u073e"+
		"\nD\fD\16D\u0741\13D\3D\3D\3E\3E\3E\3E\3E\7E\u074a\nE\fE\16E\u074d\13"+
		"E\5E\u074f\nE\3E\3E\3E\3F\5F\u0755\nF\3F\3F\5F\u0759\nF\5F\u075b\nF\3"+
		"G\3G\3G\3G\3G\3G\3G\5G\u0764\nG\3G\3G\3G\3G\3G\3G\3G\3G\3G\3G\5G\u0770"+
		"\nG\5G\u0772\nG\3G\3G\3G\3G\3G\5G\u0779\nG\3G\3G\3G\3G\3G\5G\u0780\nG"+
		"\3G\3G\3G\3G\5G\u0786\nG\3G\3G\3G\3G\5G\u078c\nG\5G\u078e\nG\3H\3H\3H"+
		"\7H\u0793\nH\fH\16H\u0796\13H\3I\3I\3I\5I\u079b\nI\3I\3I\3J\3J\3J\5J\u07a2"+
		"\nJ\3J\3J\3K\3K\5K\u07a8\nK\3K\3K\5K\u07ac\nK\5K\u07ae\nK\3L\3L\3L\7L"+
		"\u07b3\nL\fL\16L\u07b6\13L\3M\3M\3M\3M\7M\u07bc\nM\fM\16M\u07bf\13M\3"+
		"M\3M\3N\3N\3N\3N\3N\3N\7N\u07c9\nN\fN\16N\u07cc\13N\3N\3N\5N\u07d0\nN"+
		"\3O\3O\5O\u07d4\nO\3P\3P\3Q\3Q\3Q\3Q\3Q\3Q\3Q\3Q\3Q\3Q\5Q\u07e2\nQ\5Q"+
		"\u07e4\nQ\3Q\3Q\3Q\3Q\3Q\3Q\7Q\u07ec\nQ\fQ\16Q\u07ef\13Q\3R\5R\u07f2\n"+
		"R\3R\3R\3R\3R\3R\3R\5R\u07fa\nR\3R\3R\3R\3R\3R\7R\u0801\nR\fR\16R\u0804"+
		"\13R\3R\3R\3R\5R\u0809\nR\3R\3R\3R\3R\3R\3R\5R\u0811\nR\3R\3R\3R\3R\5"+
		"R\u0817\nR\3R\3R\3R\5R\u081c\nR\3R\3R\3R\5R\u0821\nR\3S\3S\3S\3S\5S\u0827"+
		"\nS\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\3S\7S\u083c"+
		"\nS\fS\16S\u083f\13S\3T\3T\3T\3T\6T\u0845\nT\rT\16T\u0846\3T\3T\5T\u084b"+
		"\nT\3T\3T\3T\3T\3T\6T\u0852\nT\rT\16T\u0853\3T\3T\5T\u0858\nT\3T\3T\3"+
		"T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\7T\u0868\nT\fT\16T\u086b\13T\5T\u086d"+
		"\nT\3T\3T\3T\3T\3T\3T\5T\u0875\nT\3T\3T\3T\3T\3T\3T\3T\5T\u087e\nT\3T"+
		"\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\3T\6T\u0893\nT\rT"+
		"\16T\u0894\3T\3T\3T\3T\3T\3T\3T\3T\3T\5T\u08a0\nT\3T\3T\3T\7T\u08a5\n"+
		"T\fT\16T\u08a8\13T\5T\u08aa\nT\3T\3T\3T\5T\u08af\nT\3T\3T\3T\3T\3T\3T"+
		"\3T\3T\3T\3T\3T\3T\3T\3T\3T\6T\u08c0\nT\rT\16T\u08c1\3T\3T\3T\3T\3T\3"+
		"T\3T\3T\3T\3T\3T\3T\3T\3T\3T\5T\u08d3\nT\3T\3T\3T\3T\3T\3T\3T\3T\7T\u08dd"+
		"\nT\fT\16T\u08e0\13T\3U\3U\3U\3U\3U\3U\3U\3U\6U\u08ea\nU\rU\16U\u08eb"+
		"\5U\u08ee\nU\3V\3V\3W\3W\3X\3X\3Y\3Y\3Z\3Z\5Z\u08fa\nZ\3Z\6Z\u08fd\nZ"+
		"\rZ\16Z\u08fe\3Z\3Z\3Z\7Z\u0904\nZ\fZ\16Z\u0907\13Z\5Z\u0909\nZ\3[\3["+
		"\3[\3[\5[\u090f\n[\3\\\5\\\u0912\n\\\3\\\3\\\5\\\u0916\n\\\3]\3]\3^\3"+
		"^\3^\5^\u091d\n^\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\5_\u092e"+
		"\n_\3_\3_\5_\u0932\n_\3_\3_\3_\3_\3_\7_\u0939\n_\f_\16_\u093c\13_\3_\5"+
		"_\u093f\n_\5_\u0941\n_\3`\3`\3`\7`\u0946\n`\f`\16`\u0949\13`\3a\3a\3a"+
		"\3a\5a\u094f\na\3b\3b\3b\7b\u0954\nb\fb\16b\u0957\13b\3c\3c\3c\3c\3c\5"+
		"c\u095e\nc\3d\3d\3d\3d\3d\3e\3e\3e\3e\7e\u0969\ne\fe\16e\u096c\13e\3f"+
		"\3f\3f\3f\3g\3g\3g\3g\3g\3g\3g\3g\3g\3g\3g\7g\u097d\ng\fg\16g\u0980\13"+
		"g\3g\3g\3g\3g\3g\7g\u0987\ng\fg\16g\u098a\13g\5g\u098c\ng\3g\3g\3g\3g"+
		"\3g\7g\u0993\ng\fg\16g\u0996\13g\5g\u0998\ng\5g\u099a\ng\3g\5g\u099d\n"+
		"g\3g\5g\u09a0\ng\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\3h\5h\u09b2"+
		"\nh\3i\3i\3i\3i\3i\3i\3i\5i\u09bb\ni\3j\3j\3j\7j\u09c0\nj\fj\16j\u09c3"+
		"\13j\3k\3k\3k\5k\u09c8\nk\3l\3l\3l\3l\3l\3l\5l\u09d0\nl\3m\3m\3n\5n\u09d5"+
		"\nn\3n\3n\5n\u09d9\nn\3n\3n\5n\u09dd\nn\3n\3n\5n\u09e1\nn\3n\3n\5n\u09e5"+
		"\nn\3n\3n\5n\u09e9\nn\3n\3n\5n\u09ed\nn\3n\5n\u09f0\nn\3o\3o\3p\3p\3q"+
		"\3q\3q\7\u0314\u0351\u0359\u0360\u0368\6P\u00a0\u00a4\u00a6r\2\4\6\b\n"+
		"\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\"+
		"^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090"+
		"\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8"+
		"\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0"+
		"\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8"+
		"\u00da\u00dc\u00de\u00e0\2\"\4\2!!\u00c2\u00c2\4\2\u00e2\u00e2\u00fc\u00fc"+
		"\7\2))\65\65WWcc\u008e\u008e\4\2ddrr\3\2GH\4\2WWcc\4\2\r\r\u0088\u0088"+
		"\4\2??\u00cc\u00cc\6\2RRyy\u00d6\u00d6\u00f4\u00f4\5\2RR\u00d6\u00d6\u00f4"+
		"\u00f4\4\2\30\30GG\4\2^^\u0080\u0080\4\2\17\17LL\3\2\u011c\u011d\4\2\u0085"+
		"\u0085\u00c5\u00c5\4\2\u010d\u010e\u0113\u0113\3\2\u010f\u0112\4\2\u010d"+
		"\u010e\u0116\u0116\4\2::<<\5\2\34\34\u0083\u0083\u00eb\u00eb\3\2\u0105"+
		"\u010c\3\2\u010d\u0117\6\2\22\22rr\u009c\u009c\u00a4\u00a4\4\2ZZ\u00ef"+
		"\u00ef\3\2\u010d\u010e\b\2ABmn\u0091\u0098\u00cd\u00ce\u00fd\u00fe\u0103"+
		"\u0104\4\2MM\u00ac\u00ac\4\2\u00a5\u00a5\u00db\u00db\4\2__\u00b2\u00b2"+
		"\66\2\r\16\20\21\25\26\30\31\33\33\35!$$&)++-\63\65\6589>@BKMNQQSY\\_"+
		"bcfhkknqstvxzz}}\177\u0082\u0085\u0094\u0096\u0096\u0098\u0099\u009b\u009b"+
		"\u009e\u009f\u00a2\u00a3\u00a6\u00a6\u00a8\u00a9\u00ab\u00b2\u00b4\u00bb"+
		"\u00bd\u00c3\u00c5\u00cc\u00ce\u00ce\u00d1\u00d3\u00d5\u00d5\u00d7\u00d9"+
		"\u00db\u00e1\u00e3\u00e7\u00ea\u00ea\u00ec\u00f3\u00f6\u00f8\u00fb\u00fe"+
		"\u0101\u0101\u0104\u0104\u0112\u0112\21\2\23\23\67\67RReeuuyy~~\u0084"+
		"\u0084\u009a\u009a\u00a0\u00a0\u00c4\u00c4\u00d0\u00d0\u00d6\u00d6\u00f4"+
		"\u00f4\u00fa\u00fa\23\2\r\22\24\668QSdftvxz}\177\u0083\u0085\u0099\u009b"+
		"\u009f\u00a1\u00c3\u00c5\u00cf\u00d1\u00d5\u00d7\u00f3\u00f5\u00f9\u00fb"+
		"\u0104\u0112\u0112\2\u0b8b\2\u00e2\3\2\2\2\4\u00e5\3\2\2\2\6\u00e8\3\2"+
		"\2\2\b\u00eb\3\2\2\2\n\u00ee\3\2\2\2\f\u00f1\3\2\2\2\16\u00f4\3\2\2\2"+
		"\20\u036b\3\2\2\2\22\u0417\3\2\2\2\24\u0419\3\2\2\2\26\u0428\3\2\2\2\30"+
		"\u0434\3\2\2\2\32\u0441\3\2\2\2\34\u0445\3\2\2\2\36\u0479\3\2\2\2 \u047b"+
		"\3\2\2\2\"\u047f\3\2\2\2$\u048b\3\2\2\2&\u0490\3\2\2\2(\u0497\3\2\2\2"+
		"*\u0499\3\2\2\2,\u04a1\3\2\2\2.\u04aa\3\2\2\2\60\u04b2\3\2\2\2\62\u04b5"+
		"\3\2\2\2\64\u04c0\3\2\2\2\66\u04d0\3\2\2\28\u04d6\3\2\2\2:\u04d8\3\2\2"+
		"\2<\u04e3\3\2\2\2>\u04f4\3\2\2\2@\u04fb\3\2\2\2B\u04fd\3\2\2\2D\u0503"+
		"\3\2\2\2F\u0510\3\2\2\2H\u051b\3\2\2\2J\u0527\3\2\2\2L\u0557\3\2\2\2N"+
		"\u055a\3\2\2\2P\u055d\3\2\2\2R\u0582\3\2\2\2T\u0584\3\2\2\2V\u05e9\3\2"+
		"\2\2X\u05eb\3\2\2\2Z\u0605\3\2\2\2\\\u0607\3\2\2\2^\u0645\3\2\2\2`\u0654"+
		"\3\2\2\2b\u0656\3\2\2\2d\u0674\3\2\2\2f\u0676\3\2\2\2h\u067d\3\2\2\2j"+
		"\u069d\3\2\2\2l\u069f\3\2\2\2n\u06b1\3\2\2\2p\u06cb\3\2\2\2r\u06d1\3\2"+
		"\2\2t\u06d3\3\2\2\2v\u06f2\3\2\2\2x\u06f4\3\2\2\2z\u06f8\3\2\2\2|\u0700"+
		"\3\2\2\2~\u070b\3\2\2\2\u0080\u070f\3\2\2\2\u0082\u071a\3\2\2\2\u0084"+
		"\u0737\3\2\2\2\u0086\u0739\3\2\2\2\u0088\u0744\3\2\2\2\u008a\u075a\3\2"+
		"\2\2\u008c\u078d\3\2\2\2\u008e\u078f\3\2\2\2\u0090\u079a\3\2\2\2\u0092"+
		"\u07a1\3\2\2\2\u0094\u07a5\3\2\2\2\u0096\u07af\3\2\2\2\u0098\u07b7\3\2"+
		"\2\2\u009a\u07cf\3\2\2\2\u009c\u07d3\3\2\2\2\u009e\u07d5\3\2\2\2\u00a0"+
		"\u07e3\3\2\2\2\u00a2\u0820\3\2\2\2\u00a4\u0826\3\2\2\2\u00a6\u08d2\3\2"+
		"\2\2\u00a8\u08ed\3\2\2\2\u00aa\u08ef\3\2\2\2\u00ac\u08f1\3\2\2\2\u00ae"+
		"\u08f3\3\2\2\2\u00b0\u08f5\3\2\2\2\u00b2\u0908\3\2\2\2\u00b4\u090a\3\2"+
		"\2\2\u00b6\u0915\3\2\2\2\u00b8\u0917\3\2\2\2\u00ba\u091c\3\2\2\2\u00bc"+
		"\u0940\3\2\2\2\u00be\u0942\3\2\2\2\u00c0\u094a\3\2\2\2\u00c2\u0950\3\2"+
		"\2\2\u00c4\u0958\3\2\2\2\u00c6\u095f\3\2\2\2\u00c8\u0964\3\2\2\2\u00ca"+
		"\u096d\3\2\2\2\u00cc\u099f\3\2\2\2\u00ce\u09b1\3\2\2\2\u00d0\u09ba\3\2"+
		"\2\2\u00d2\u09bc\3\2\2\2\u00d4\u09c7\3\2\2\2\u00d6\u09cf\3\2\2\2\u00d8"+
		"\u09d1\3\2\2\2\u00da\u09ef\3\2\2\2\u00dc\u09f1\3\2\2\2\u00de\u09f3\3\2"+
		"\2\2\u00e0\u09f5\3\2\2\2\u00e2\u00e3\5\20\t\2\u00e3\u00e4\7\2\2\3\u00e4"+
		"\3\3\2\2\2\u00e5\u00e6\5\u0094K\2\u00e6\u00e7\7\2\2\3\u00e7\5\3\2\2\2"+
		"\u00e8\u00e9\5\u0090I\2\u00e9\u00ea\7\2\2\3\u00ea\7\3\2\2\2\u00eb\u00ec"+
		"\5\u008eH\2\u00ec\u00ed\7\2\2\3\u00ed\t\3\2\2\2\u00ee\u00ef\5\u0092J\2"+
		"\u00ef\u00f0\7\2\2\3\u00f0\13\3\2\2\2\u00f1\u00f2\5\u00bc_\2\u00f2\u00f3"+
		"\7\2\2\3\u00f3\r\3\2\2\2\u00f4\u00f5\5\u00be`\2\u00f5\u00f6\7\2\2\3\u00f6"+
		"\17\3\2\2\2\u00f7\u036c\5\34\17\2\u00f8\u00fa\5,\27\2\u00f9\u00f8\3\2"+
		"\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u036c\5F$\2\u00fc\u00fd"+
		"\7\u00f8\2\2\u00fd\u036c\5\u00d4k\2\u00fe\u00ff\7\66\2\2\u00ff\u0103\5"+
		"&\24\2\u0100\u0101\7o\2\2\u0101\u0102\7\u009c\2\2\u0102\u0104\7T\2\2\u0103"+
		"\u0100\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0108\5\u00d4"+
		"k\2\u0106\u0107\7.\2\2\u0107\u0109\7\u0118\2\2\u0108\u0106\3\2\2\2\u0108"+
		"\u0109\3\2\2\2\u0109\u010b\3\2\2\2\u010a\u010c\5\32\16\2\u010b\u010a\3"+
		"\2\2\2\u010b\u010c\3\2\2\2\u010c\u0110\3\2\2\2\u010d\u010e\7\u0102\2\2"+
		"\u010e\u010f\7C\2\2\u010f\u0111\5\62\32\2\u0110\u010d\3\2\2\2\u0110\u0111"+
		"\3\2\2\2\u0111\u036c\3\2\2\2\u0112\u0113\7\20\2\2\u0113\u0114\5&\24\2"+
		"\u0114\u0115\5\u00d4k\2\u0115\u0116\7\u00d5\2\2\u0116\u0117\7C\2\2\u0117"+
		"\u0118\5\62\32\2\u0118\u036c\3\2\2\2\u0119\u011a\7N\2\2\u011a\u011d\5"+
		"&\24\2\u011b\u011c\7o\2\2\u011c\u011e\7T\2\2\u011d\u011b\3\2\2\2\u011d"+
		"\u011e\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\5\u00d4k\2\u0120\u0122"+
		"\t\2\2\2\u0121\u0120\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u036c\3\2\2\2\u0123"+
		"\u0124\7\u00d8\2\2\u0124\u0129\7@\2\2\u0125\u0127\7\u0085\2\2\u0126\u0125"+
		"\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0128\3\2\2\2\u0128\u012a\7\u0118\2"+
		"\2\u0129\u0126\3\2\2\2\u0129\u012a\3\2\2\2\u012a\u036c\3\2\2\2\u012b\u0130"+
		"\5\24\13\2\u012c\u012d\7\3\2\2\u012d\u012e\5\u00be`\2\u012e\u012f\7\4"+
		"\2\2\u012f\u0131\3\2\2\2\u0130\u012c\3\2\2\2\u0130\u0131\3\2\2\2\u0131"+
		"\u0132\3\2\2\2\u0132\u0140\5\60\31\2\u0133\u0134\7\u00a3\2\2\u0134\u013f"+
		"\5\62\32\2\u0135\u0136\7\u00ad\2\2\u0136\u0137\7\37\2\2\u0137\u013f\5"+
		"\u0098M\2\u0138\u013f\5\26\f\2\u0139\u013f\5\32\16\2\u013a\u013b\7.\2"+
		"\2\u013b\u013f\7\u0118\2\2\u013c\u013d\7\u00e5\2\2\u013d\u013f\5\62\32"+
		"\2\u013e\u0133\3\2\2\2\u013e\u0135\3\2\2\2\u013e\u0138\3\2\2\2\u013e\u0139"+
		"\3\2\2\2\u013e\u013a\3\2\2\2\u013e\u013c\3\2\2\2\u013f\u0142\3\2\2\2\u0140"+
		"\u013e\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u0147\3\2\2\2\u0142\u0140\3\2"+
		"\2\2\u0143\u0145\7\27\2\2\u0144\u0143\3\2\2\2\u0144\u0145\3\2\2\2\u0145"+
		"\u0146\3\2\2\2\u0146\u0148\5\34\17\2\u0147\u0144\3\2\2\2\u0147\u0148\3"+
		"\2\2\2\u0148\u036c\3\2\2\2\u0149\u014e\5\24\13\2\u014a\u014b\7\3\2\2\u014b"+
		"\u014c\5\u00be`\2\u014c\u014d\7\4\2\2\u014d\u014f\3\2\2\2\u014e\u014a"+
		"\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u0166\3\2\2\2\u0150\u0151\7.\2\2\u0151"+
		"\u0165\7\u0118\2\2\u0152\u0153\7\u00ad\2\2\u0153\u0154\7\37\2\2\u0154"+
		"\u0155\7\3\2\2\u0155\u0156\5\u00be`\2\u0156\u0157\7\4\2\2\u0157\u015c"+
		"\3\2\2\2\u0158\u0159\7\u00ad\2\2\u0159\u015a\7\37\2\2\u015a\u015c\5x="+
		"\2\u015b\u0152\3\2\2\2\u015b\u0158\3\2\2\2\u015c\u0165\3\2\2\2\u015d\u0165"+
		"\5\26\f\2\u015e\u0165\5\30\r\2\u015f\u0165\5\u008cG\2\u0160\u0165\5> "+
		"\2\u0161\u0165\5\32\16\2\u0162\u0163\7\u00e5\2\2\u0163\u0165\5\62\32\2"+
		"\u0164\u0150\3\2\2\2\u0164\u015b\3\2\2\2\u0164\u015d\3\2\2\2\u0164\u015e"+
		"\3\2\2\2\u0164\u015f\3\2\2\2\u0164\u0160\3\2\2\2\u0164\u0161\3\2\2\2\u0164"+
		"\u0162\3\2\2\2\u0165\u0168\3\2\2\2\u0166\u0164\3\2\2\2\u0166\u0167\3\2"+
		"\2\2\u0167\u016d\3\2\2\2\u0168\u0166\3\2\2\2\u0169\u016b\7\27\2\2\u016a"+
		"\u0169\3\2\2\2\u016a\u016b\3\2\2\2\u016b\u016c\3\2\2\2\u016c\u016e\5\34"+
		"\17\2\u016d\u016a\3\2\2\2\u016d\u016e\3\2\2\2\u016e\u036c\3\2\2\2\u016f"+
		"\u0170\7\66\2\2\u0170\u0174\7\u00e2\2\2\u0171\u0172\7o\2\2\u0172\u0173"+
		"\7\u009c\2\2\u0173\u0175\7T\2\2\u0174\u0171\3\2\2\2\u0174\u0175\3\2\2"+
		"\2\u0175\u0176\3\2\2\2\u0176\u0177\5\u0090I\2\u0177\u0178\7\u0085\2\2"+
		"\u0178\u017a\5\u0090I\2\u0179\u017b\5\32\16\2\u017a\u0179\3\2\2\2\u017a"+
		"\u017b\3\2\2\2\u017b\u036c\3\2\2\2\u017c\u017d\7\21\2\2\u017d\u017e\7"+
		"\u00e2\2\2\u017e\u0180\5\u0090I\2\u017f\u0181\5\"\22\2\u0180\u017f\3\2"+
		"\2\2\u0180\u0181\3\2\2\2\u0181\u0182\3\2\2\2\u0182\u0183\7\62\2\2\u0183"+
		"\u018b\7\u00de\2\2\u0184\u018c\5\u00d4k\2\u0185\u0186\7`\2\2\u0186\u0187"+
		"\7-\2\2\u0187\u018c\5z>\2\u0188\u0189\7`\2\2\u0189\u018a\7\17\2\2\u018a"+
		"\u018c\7-\2\2\u018b\u0184\3\2\2\2\u018b\u0185\3\2\2\2\u018b\u0188\3\2"+
		"\2\2\u018b\u018c\3\2\2\2\u018c\u036c\3\2\2\2\u018d\u018e\7\20\2\2\u018e"+
		"\u018f\7\u00e2\2\2\u018f\u0190\5\u0090I\2\u0190\u0191\7\r\2\2\u0191\u0192"+
		"\7-\2\2\u0192\u0193\7\3\2\2\u0193\u0194\5\u00be`\2\u0194\u0195\7\4\2\2"+
		"\u0195\u036c\3\2\2\2\u0196\u0197\7\20\2\2\u0197\u0198\t\3\2\2\u0198\u0199"+
		"\5\u0090I\2\u0199\u019a\7\u00be\2\2\u019a\u019b\7\u00e9\2\2\u019b\u019c"+
		"\5\u0090I\2\u019c\u036c\3\2\2\2\u019d\u019e\7\20\2\2\u019e\u019f\t\3\2"+
		"\2\u019f\u01a0\5\u0090I\2\u01a0\u01a1\7\u00d5\2\2\u01a1\u01a2\7\u00e5"+
		"\2\2\u01a2\u01a3\5\62\32\2\u01a3\u036c\3\2\2\2\u01a4\u01a5\7\20\2\2\u01a5"+
		"\u01a6\t\3\2\2\u01a6\u01a7\5\u0090I\2\u01a7\u01a8\7\u00f7\2\2\u01a8\u01ab"+
		"\7\u00e5\2\2\u01a9\u01aa\7o\2\2\u01aa\u01ac\7T\2\2\u01ab\u01a9\3\2\2\2"+
		"\u01ab\u01ac\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01ae\5\62\32\2\u01ae\u036c"+
		"\3\2\2\2\u01af\u01b0\7\20\2\2\u01b0\u01b1\7\u00e2\2\2\u01b1\u01b3\5\u0090"+
		"I\2\u01b2\u01b4\5\"\22\2\u01b3\u01b2\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4"+
		"\u01b5\3\2\2\2\u01b5\u01b7\7$\2\2\u01b6\u01b8\7,\2\2\u01b7\u01b6\3\2\2"+
		"\2\u01b7\u01b8\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9\u01ba\5\u00d4k\2\u01ba"+
		"\u01bc\5\u00c0a\2\u01bb\u01bd\5\u00ba^\2\u01bc\u01bb\3\2\2\2\u01bc\u01bd"+
		"\3\2\2\2\u01bd\u036c\3\2\2\2\u01be\u01bf\7\20\2\2\u01bf\u01c0\7\u00e2"+
		"\2\2\u01c0\u01c2\5\u0090I\2\u01c1\u01c3\5\"\22\2\u01c2\u01c1\3\2\2\2\u01c2"+
		"\u01c3\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c5\7\u00d5\2\2\u01c5\u01c6"+
		"\7\u00d2\2\2\u01c6\u01ca\7\u0118\2\2\u01c7\u01c8\7\u0102\2\2\u01c8\u01c9"+
		"\7\u00d3\2\2\u01c9\u01cb\5\62\32\2\u01ca\u01c7\3\2\2\2\u01ca\u01cb\3\2"+
		"\2\2\u01cb\u036c\3\2\2\2\u01cc\u01cd\7\20\2\2\u01cd\u01ce\7\u00e2\2\2"+
		"\u01ce\u01d0\5\u0090I\2\u01cf\u01d1\5\"\22\2\u01d0\u01cf\3\2\2\2\u01d0"+
		"\u01d1\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\u01d3\7\u00d5\2\2\u01d3\u01d4"+
		"\7\u00d3\2\2\u01d4\u01d5\5\62\32\2\u01d5\u036c\3\2\2\2\u01d6\u01d7\7\20"+
		"\2\2\u01d7\u01d8\7\u00e2\2\2\u01d8\u01d9\5\u0090I\2\u01d9\u01dd\7\r\2"+
		"\2\u01da\u01db\7o\2\2\u01db\u01dc\7\u009c\2\2\u01dc\u01de\7T\2\2\u01dd"+
		"\u01da\3\2\2\2\u01dd\u01de\3\2\2\2\u01de\u01e0\3\2\2\2\u01df\u01e1\5 "+
		"\21\2\u01e0\u01df\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e0\3\2\2\2\u01e2"+
		"\u01e3\3\2\2\2\u01e3\u036c\3\2\2\2\u01e4\u01e5\7\20\2\2\u01e5\u01e6\7"+
		"\u00fc\2\2\u01e6\u01e7\5\u0090I\2\u01e7\u01eb\7\r\2\2\u01e8\u01e9\7o\2"+
		"\2\u01e9\u01ea\7\u009c\2\2\u01ea\u01ec\7T\2\2\u01eb\u01e8\3\2\2\2\u01eb"+
		"\u01ec\3\2\2\2\u01ec\u01ee\3\2\2\2\u01ed\u01ef\5\"\22\2\u01ee\u01ed\3"+
		"\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01ee\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1"+
		"\u036c\3\2\2\2\u01f2\u01f3\7\20\2\2\u01f3\u01f4\7\u00e2\2\2\u01f4\u01f5"+
		"\5\u0090I\2\u01f5\u01f6\5\"\22\2\u01f6\u01f7\7\u00be\2\2\u01f7\u01f8\7"+
		"\u00e9\2\2\u01f8\u01f9\5\"\22\2\u01f9\u036c\3\2\2\2\u01fa\u01fb\7\20\2"+
		"\2\u01fb\u01fc\7\u00e2\2\2\u01fc\u01fd\5\u0090I\2\u01fd\u0200\7N\2\2\u01fe"+
		"\u01ff\7o\2\2\u01ff\u0201\7T\2\2\u0200\u01fe\3\2\2\2\u0200\u0201\3\2\2"+
		"\2\u0201\u0202\3\2\2\2\u0202\u0207\5\"\22\2\u0203\u0204\7\5\2\2\u0204"+
		"\u0206\5\"\22\2\u0205\u0203\3\2\2\2\u0206\u0209\3\2\2\2\u0207\u0205\3"+
		"\2\2\2\u0207\u0208\3\2\2\2\u0208\u020b\3\2\2\2\u0209\u0207\3\2\2\2\u020a"+
		"\u020c\7\u00b5\2\2\u020b\u020a\3\2\2\2\u020b\u020c\3\2\2\2\u020c\u036c"+
		"\3\2\2\2\u020d\u020e\7\20\2\2\u020e\u020f\7\u00fc\2\2\u020f\u0210\5\u0090"+
		"I\2\u0210\u0213\7N\2\2\u0211\u0212\7o\2\2\u0212\u0214\7T\2\2\u0213\u0211"+
		"\3\2\2\2\u0213\u0214\3\2\2\2\u0214\u0215\3\2\2\2\u0215\u021a\5\"\22\2"+
		"\u0216\u0217\7\5\2\2\u0217\u0219\5\"\22\2\u0218\u0216\3\2\2\2\u0219\u021c"+
		"\3\2\2\2\u021a\u0218\3\2\2\2\u021a\u021b\3\2\2\2\u021b\u036c\3\2\2\2\u021c"+
		"\u021a\3\2\2\2\u021d\u021e\7\20\2\2\u021e\u021f\7\u00e2\2\2\u021f\u0221"+
		"\5\u0090I\2\u0220\u0222\5\"\22\2\u0221\u0220\3\2\2\2\u0221\u0222\3\2\2"+
		"\2\u0222\u0223\3\2\2\2\u0223\u0224\7\u00d5\2\2\u0224\u0225\5\32\16\2\u0225"+
		"\u036c\3\2\2\2\u0226\u0227\7\20\2\2\u0227\u0228\7\u00e2\2\2\u0228\u0229"+
		"\5\u0090I\2\u0229\u022a\7\u00ba\2\2\u022a\u022b\7\u00ae\2\2\u022b\u036c"+
		"\3\2\2\2\u022c\u022d\7N\2\2\u022d\u0230\7\u00e2\2\2\u022e\u022f\7o\2\2"+
		"\u022f\u0231\7T\2\2\u0230\u022e\3\2\2\2\u0230\u0231\3\2\2\2\u0231\u0232"+
		"\3\2\2\2\u0232\u0234\5\u0090I\2\u0233\u0235\7\u00b5\2\2\u0234\u0233\3"+
		"\2\2\2\u0234\u0235\3\2\2\2\u0235\u036c\3\2\2\2\u0236\u0237\7N\2\2\u0237"+
		"\u023a\7\u00fc\2\2\u0238\u0239\7o\2\2\u0239\u023b\7T\2\2\u023a\u0238\3"+
		"\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u036c\5\u0090I\2"+
		"\u023d\u0240\7\66\2\2\u023e\u023f\7\u00a4\2\2\u023f\u0241\7\u00c0\2\2"+
		"\u0240\u023e\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u0246\3\2\2\2\u0242\u0244"+
		"\7h\2\2\u0243\u0242\3\2\2\2\u0243\u0244\3\2\2\2\u0244\u0245\3\2\2\2\u0245"+
		"\u0247\7\u00e6\2\2\u0246\u0243\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0248"+
		"\3\2\2\2\u0248\u024c\7\u00fc\2\2\u0249\u024a\7o\2\2\u024a\u024b\7\u009c"+
		"\2\2\u024b\u024d\7T\2\2\u024c\u0249\3\2\2\2\u024c\u024d\3\2\2\2\u024d"+
		"\u024e\3\2\2\2\u024e\u0250\5\u0090I\2\u024f\u0251\5\u0080A\2\u0250\u024f"+
		"\3\2\2\2\u0250\u0251\3\2\2\2\u0251\u0254\3\2\2\2\u0252\u0253\7.\2\2\u0253"+
		"\u0255\7\u0118\2\2\u0254\u0252\3\2\2\2\u0254\u0255\3\2\2\2\u0255\u0259"+
		"\3\2\2\2\u0256\u0257\7\u00ad\2\2\u0257\u0258\7\u00a0\2\2\u0258\u025a\5"+
		"x=\2\u0259\u0256\3\2\2\2\u0259\u025a\3\2\2\2\u025a\u025d\3\2\2\2\u025b"+
		"\u025c\7\u00e5\2\2\u025c\u025e\5\62\32\2\u025d\u025b\3\2\2\2\u025d\u025e"+
		"\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u0260\7\27\2\2\u0260\u0261\5\34\17"+
		"\2\u0261\u036c\3\2\2\2\u0262\u0265\7\66\2\2\u0263\u0264\7\u00a4\2\2\u0264"+
		"\u0266\7\u00c0\2\2\u0265\u0263\3\2\2\2\u0265\u0266\3\2\2\2\u0266\u0268"+
		"\3\2\2\2\u0267\u0269\7h\2\2\u0268\u0267\3\2\2\2\u0268\u0269\3\2\2\2\u0269"+
		"\u026a\3\2\2\2\u026a\u026b\7\u00e6\2\2\u026b\u026c\7\u00fc\2\2\u026c\u0271"+
		"\5\u0090I\2\u026d\u026e\7\3\2\2\u026e\u026f\5\u00be`\2\u026f\u0270\7\4"+
		"\2\2\u0270\u0272\3\2\2\2\u0271\u026d\3\2\2\2\u0271\u0272\3\2\2\2\u0272"+
		"\u0273\3\2\2\2\u0273\u0276\5\60\31\2\u0274\u0275\7\u00a3\2\2\u0275\u0277"+
		"\5\62\32\2\u0276\u0274\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u036c\3\2\2\2"+
		"\u0278\u0279\7\20\2\2\u0279\u027a\7\u00fc\2\2\u027a\u027c\5\u0090I\2\u027b"+
		"\u027d\7\27\2\2\u027c\u027b\3\2\2\2\u027c\u027d\3\2\2\2\u027d\u027e\3"+
		"\2\2\2\u027e\u027f\5\34\17\2\u027f\u036c\3\2\2\2\u0280\u0283\7\66\2\2"+
		"\u0281\u0282\7\u00a4\2\2\u0282\u0284\7\u00c0\2\2\u0283\u0281\3\2\2\2\u0283"+
		"\u0284\3\2\2\2\u0284\u0286\3\2\2\2\u0285\u0287\7\u00e6\2\2\u0286\u0285"+
		"\3\2\2\2\u0286\u0287\3\2\2\2\u0287\u0288\3\2\2\2\u0288\u028c\7f\2\2\u0289"+
		"\u028a\7o\2\2\u028a\u028b\7\u009c\2\2\u028b\u028d\7T\2\2\u028c\u0289\3"+
		"\2\2\2\u028c\u028d\3\2\2\2\u028d\u028e\3\2\2\2\u028e\u028f\5\u00d2j\2"+
		"\u028f\u0290\7\27\2\2\u0290\u029a\7\u0118\2\2\u0291\u0292\7\u00fa\2\2"+
		"\u0292\u0297\5D#\2\u0293\u0294\7\5\2\2\u0294\u0296\5D#\2\u0295\u0293\3"+
		"\2\2\2\u0296\u0299\3\2\2\2\u0297\u0295\3\2\2\2\u0297\u0298\3\2\2\2\u0298"+
		"\u029b\3\2\2\2\u0299\u0297\3\2\2\2\u029a\u0291\3\2\2\2\u029a\u029b\3\2"+
		"\2\2\u029b\u036c\3\2\2\2\u029c\u029e\7N\2\2\u029d\u029f\7\u00e6\2\2\u029e"+
		"\u029d\3\2\2\2\u029e\u029f\3\2\2\2\u029f\u02a0\3\2\2\2\u02a0\u02a3\7f"+
		"\2\2\u02a1\u02a2\7o\2\2\u02a2\u02a4\7T\2\2\u02a3\u02a1\3\2\2\2\u02a3\u02a4"+
		"\3\2\2\2\u02a4\u02a5\3\2\2\2\u02a5\u036c\5\u00d2j\2\u02a6\u02a8\7U\2\2"+
		"\u02a7\u02a9\t\4\2\2\u02a8\u02a7\3\2\2\2\u02a8\u02a9\3\2\2\2\u02a9\u02aa"+
		"\3\2\2\2\u02aa\u036c\5\20\t\2\u02ab\u02ac\7\u00d8\2\2\u02ac\u02af\7\u00e3"+
		"\2\2\u02ad\u02ae\t\5\2\2\u02ae\u02b0\5\u00d4k\2\u02af\u02ad\3\2\2\2\u02af"+
		"\u02b0\3\2\2\2\u02b0\u02b5\3\2\2\2\u02b1\u02b3\7\u0085\2\2\u02b2\u02b1"+
		"\3\2\2\2\u02b2\u02b3\3\2\2\2\u02b3\u02b4\3\2\2\2\u02b4\u02b6\7\u0118\2"+
		"\2\u02b5\u02b2\3\2\2\2\u02b5\u02b6\3\2\2\2\u02b6\u036c\3\2\2\2\u02b7\u02b8"+
		"\7\u00d8\2\2\u02b8\u02b9\7\u00e2\2\2\u02b9\u02bc\7W\2\2\u02ba\u02bb\t"+
		"\5\2\2\u02bb\u02bd\5\u00d4k\2\u02bc\u02ba\3\2\2\2\u02bc\u02bd\3\2\2\2"+
		"\u02bd\u02be\3\2\2\2\u02be\u02bf\7\u0085\2\2\u02bf\u02c1\7\u0118\2\2\u02c0"+
		"\u02c2\5\"\22\2\u02c1\u02c0\3\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u036c\3"+
		"\2\2\2\u02c3\u02c4\7\u00d8\2\2\u02c4\u02c5\7\u00e5\2\2\u02c5\u02ca\5\u0090"+
		"I\2\u02c6\u02c7\7\3\2\2\u02c7\u02c8\5\66\34\2\u02c8\u02c9\7\4\2\2\u02c9"+
		"\u02cb\3\2\2\2\u02ca\u02c6\3\2\2\2\u02ca\u02cb\3\2\2\2\u02cb\u036c\3\2"+
		"\2\2\u02cc\u02cd\7\u00d8\2\2\u02cd\u02ce\7-\2\2\u02ce\u02cf\t\5\2\2\u02cf"+
		"\u02d2\5\u0090I\2\u02d0\u02d1\t\5\2\2\u02d1\u02d3\5\u00d4k\2\u02d2\u02d0"+
		"\3\2\2\2\u02d2\u02d3\3\2\2\2\u02d3\u036c\3\2\2\2\u02d4\u02d5\7\u00d8\2"+
		"\2\u02d5\u02d6\7\u00ae\2\2\u02d6\u02d8\5\u0090I\2\u02d7\u02d9\5\"\22\2"+
		"\u02d8\u02d7\3\2\2\2\u02d8\u02d9\3\2\2\2\u02d9\u036c\3\2\2\2\u02da\u02dc"+
		"\7\u00d8\2\2\u02db\u02dd\5\u00d4k\2\u02dc\u02db\3\2\2\2\u02dc\u02dd\3"+
		"\2\2\2\u02dd\u02de\3\2\2\2\u02de\u02e6\7g\2\2\u02df\u02e1\7\u0085\2\2"+
		"\u02e0\u02df\3\2\2\2\u02e0\u02e1\3\2\2\2\u02e1\u02e4\3\2\2\2\u02e2\u02e5"+
		"\5\u00d2j\2\u02e3\u02e5\7\u0118\2\2\u02e4\u02e2\3\2\2\2\u02e4\u02e3\3"+
		"\2\2\2\u02e5\u02e7\3\2\2\2\u02e6\u02e0\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7"+
		"\u036c\3\2\2\2\u02e8\u02e9\7\u00d8\2\2\u02e9\u02ea\7\66\2\2\u02ea\u02eb"+
		"\7\u00e2\2\2\u02eb\u036c\5\u0090I\2\u02ec\u02ed\t\6\2\2\u02ed\u02ef\7"+
		"f\2\2\u02ee\u02f0\7W\2\2\u02ef\u02ee\3\2\2\2\u02ef\u02f0\3\2\2\2\u02f0"+
		"\u02f1\3\2\2\2\u02f1\u036c\5(\25\2\u02f2\u02f3\t\6\2\2\u02f3\u02f5\5&"+
		"\24\2\u02f4\u02f6\7W\2\2\u02f5\u02f4\3\2\2\2\u02f5\u02f6\3\2\2\2\u02f6"+
		"\u02f7\3\2\2\2\u02f7\u02f8\5\u00d4k\2\u02f8\u036c\3\2\2\2\u02f9\u02fb"+
		"\t\6\2\2\u02fa\u02fc\7\u00e2\2\2\u02fb\u02fa\3\2\2\2\u02fb\u02fc\3\2\2"+
		"\2\u02fc\u02fe\3\2\2\2\u02fd\u02ff\t\7\2\2\u02fe\u02fd\3\2\2\2\u02fe\u02ff"+
		"\3\2\2\2\u02ff\u0300\3\2\2\2\u0300\u0302\5\u0090I\2\u0301\u0303\5\"\22"+
		"\2\u0302\u0301\3\2\2\2\u0302\u0303\3\2\2\2\u0303\u0305\3\2\2\2\u0304\u0306"+
		"\5*\26\2\u0305\u0304\3\2\2\2\u0305\u0306\3\2\2\2\u0306\u036c\3\2\2\2\u0307"+
		"\u0309\t\6\2\2\u0308\u030a\7\u00b6\2\2\u0309\u0308\3\2\2\2\u0309\u030a"+
		"\3\2\2\2\u030a\u030b\3\2\2\2\u030b\u036c\5\34\17\2\u030c\u030d\7\u00bd"+
		"\2\2\u030d\u030e\7\u00e2\2\2\u030e\u036c\5\u0090I\2\u030f\u0317\7\u00bd"+
		"\2\2\u0310\u0318\7\u0118\2\2\u0311\u0313\13\2\2\2\u0312\u0311\3\2\2\2"+
		"\u0313\u0316\3\2\2\2\u0314\u0315\3\2\2\2\u0314\u0312\3\2\2\2\u0315\u0318"+
		"\3\2\2\2\u0316\u0314\3\2\2\2\u0317\u0310\3\2\2\2\u0317\u0314\3\2\2\2\u0318"+
		"\u036c\3\2\2\2\u0319\u031b\7 \2\2\u031a\u031c\7\u0082\2\2\u031b\u031a"+
		"\3\2\2\2\u031b\u031c\3\2\2\2\u031c\u031d\3\2\2\2\u031d\u031e\7\u00e2\2"+
		"\2\u031e\u0321\5\u0090I\2\u031f\u0320\7\u00a3\2\2\u0320\u0322\5\62\32"+
		"\2\u0321\u031f\3\2\2\2\u0321\u0322\3\2\2\2\u0322\u0327\3\2\2\2\u0323\u0325"+
		"\7\27\2\2\u0324\u0323\3\2\2\2\u0324\u0325\3\2\2\2\u0325\u0326\3\2\2\2"+
		"\u0326\u0328\5\34\17\2\u0327\u0324\3\2\2\2\u0327\u0328\3\2\2\2\u0328\u036c"+
		"\3\2\2\2\u0329\u032a\7\u00f3\2\2\u032a\u032d\7\u00e2\2\2\u032b\u032c\7"+
		"o\2\2\u032c\u032e\7T\2\2\u032d\u032b\3\2\2\2\u032d\u032e\3\2\2\2\u032e"+
		"\u032f\3\2\2\2\u032f\u036c\5\u0090I\2\u0330\u0331\7&\2\2\u0331\u036c\7"+
		" \2\2\u0332\u0333\7\u0089\2\2\u0333\u0335\7>\2\2\u0334\u0336\7\u008a\2"+
		"\2\u0335\u0334\3\2\2\2\u0335\u0336\3\2\2\2\u0336\u0337\3\2\2\2\u0337\u0338"+
		"\7v\2\2\u0338\u033a\7\u0118\2\2\u0339\u033b\7\u00ab\2\2\u033a\u0339\3"+
		"\2\2\2\u033a\u033b\3\2\2\2\u033b\u033c\3\2\2\2\u033c\u033d\7{\2\2\u033d"+
		"\u033e\7\u00e2\2\2\u033e\u0340\5\u0090I\2\u033f\u0341\5\"\22\2\u0340\u033f"+
		"\3\2\2\2\u0340\u0341\3\2\2\2\u0341\u036c\3\2\2\2\u0342\u0343\7\u00f0\2"+
		"\2\u0343\u0344\7\u00e2\2\2\u0344\u0346\5\u0090I\2\u0345\u0347\5\"\22\2"+
		"\u0346\u0345\3\2\2\2\u0346\u0347\3\2\2\2\u0347\u036c\3\2\2\2\u0348\u0349"+
		"\7\u0099\2\2\u0349\u034a\7\u00bf\2\2\u034a\u034b\7\u00e2\2\2\u034b\u036c"+
		"\5\u0090I\2\u034c\u034d\t\b\2\2\u034d\u0351\5\u00d4k\2\u034e\u0350\13"+
		"\2\2\2\u034f\u034e\3\2\2\2\u0350\u0353\3\2\2\2\u0351\u0352\3\2\2\2\u0351"+
		"\u034f\3\2\2\2\u0352\u036c\3\2\2\2\u0353\u0351\3\2\2\2\u0354\u0355\7\u00d5"+
		"\2\2\u0355\u0359\7\u00c6\2\2\u0356\u0358\13\2\2\2\u0357\u0356\3\2\2\2"+
		"\u0358\u035b\3\2\2\2\u0359\u035a\3\2\2\2\u0359\u0357\3\2\2\2\u035a\u036c"+
		"\3\2\2\2\u035b\u0359\3\2\2\2\u035c\u0360\7\u00d5\2\2\u035d\u035f\13\2"+
		"\2\2\u035e\u035d\3\2\2\2\u035f\u0362\3\2\2\2\u0360\u0361\3\2\2\2\u0360"+
		"\u035e\3\2\2\2\u0361\u036c\3\2\2\2\u0362\u0360\3\2\2\2\u0363\u036c\7\u00c1"+
		"\2\2\u0364\u0368\5\22\n\2\u0365\u0367\13\2\2\2\u0366\u0365\3\2\2\2\u0367"+
		"\u036a\3\2\2\2\u0368\u0369\3\2\2\2\u0368\u0366\3\2\2\2\u0369\u036c\3\2"+
		"\2\2\u036a\u0368\3\2\2\2\u036b\u00f7\3\2\2\2\u036b\u00f9\3\2\2\2\u036b"+
		"\u00fc\3\2\2\2\u036b\u00fe\3\2\2\2\u036b\u0112\3\2\2\2\u036b\u0119\3\2"+
		"\2\2\u036b\u0123\3\2\2\2\u036b\u012b\3\2\2\2\u036b\u0149\3\2\2\2\u036b"+
		"\u016f\3\2\2\2\u036b\u017c\3\2\2\2\u036b\u018d\3\2\2\2\u036b\u0196\3\2"+
		"\2\2\u036b\u019d\3\2\2\2\u036b\u01a4\3\2\2\2\u036b\u01af\3\2\2\2\u036b"+
		"\u01be\3\2\2\2\u036b\u01cc\3\2\2\2\u036b\u01d6\3\2\2\2\u036b\u01e4\3\2"+
		"\2\2\u036b\u01f2\3\2\2\2\u036b\u01fa\3\2\2\2\u036b\u020d\3\2\2\2\u036b"+
		"\u021d\3\2\2\2\u036b\u0226\3\2\2\2\u036b\u022c\3\2\2\2\u036b\u0236\3\2"+
		"\2\2\u036b\u023d\3\2\2\2\u036b\u0262\3\2\2\2\u036b\u0278\3\2\2\2\u036b"+
		"\u0280\3\2\2\2\u036b\u029c\3\2\2\2\u036b\u02a6\3\2\2\2\u036b\u02ab\3\2"+
		"\2\2\u036b\u02b7\3\2\2\2\u036b\u02c3\3\2\2\2\u036b\u02cc\3\2\2\2\u036b"+
		"\u02d4\3\2\2\2\u036b\u02da\3\2\2\2\u036b\u02e8\3\2\2\2\u036b\u02ec\3\2"+
		"\2\2\u036b\u02f2\3\2\2\2\u036b\u02f9\3\2\2\2\u036b\u0307\3\2\2\2\u036b"+
		"\u030c\3\2\2\2\u036b\u030f\3\2\2\2\u036b\u0319\3\2\2\2\u036b\u0329\3\2"+
		"\2\2\u036b\u0330\3\2\2\2\u036b\u0332\3\2\2\2\u036b\u0342\3\2\2\2\u036b"+
		"\u0348\3\2\2\2\u036b\u034c\3\2\2\2\u036b\u0354\3\2\2\2\u036b\u035c\3\2"+
		"\2\2\u036b\u0363\3\2\2\2\u036b\u0364\3\2\2\2\u036c\21\3\2\2\2\u036d\u036e"+
		"\7\66\2\2\u036e\u0418\7\u00c6\2\2\u036f\u0370\7N\2\2\u0370\u0418\7\u00c6"+
		"\2\2\u0371\u0373\7i\2\2\u0372\u0374\7\u00c6\2\2\u0373\u0372\3\2\2\2\u0373"+
		"\u0374\3\2\2\2\u0374\u0418\3\2\2\2\u0375\u0377\7\u00c3\2\2\u0376\u0378"+
		"\7\u00c6\2\2\u0377\u0376\3\2\2\2\u0377\u0378\3\2\2\2\u0378\u0418\3\2\2"+
		"\2\u0379\u037a\7\u00d8\2\2\u037a\u0418\7i\2\2\u037b\u037c\7\u00d8\2\2"+
		"\u037c\u037e\7\u00c6\2\2\u037d\u037f\7i\2\2\u037e\u037d\3\2\2\2\u037e"+
		"\u037f\3\2\2\2\u037f\u0418\3\2\2\2\u0380\u0381\7\u00d8\2\2\u0381\u0418"+
		"\7\u00b4\2\2\u0382\u0383\7\u00d8\2\2\u0383\u0418\7\u00c7\2\2\u0384\u0385"+
		"\7\u00d8\2\2\u0385\u0386\79\2\2\u0386\u0418\7\u00c7\2\2\u0387\u0388\7"+
		"V\2\2\u0388\u0418\7\u00e2\2\2\u0389\u038a\7q\2\2\u038a\u0418\7\u00e2\2"+
		"\2\u038b\u038c\7\u00d8\2\2\u038c\u0418\7\61\2\2\u038d\u038e\7\u00d8\2"+
		"\2\u038e\u038f\7\66\2\2\u038f\u0418\7\u00e2\2\2\u0390\u0391\7\u00d8\2"+
		"\2\u0391\u0418\7\u00ed\2\2\u0392\u0393\7\u00d8\2\2\u0393\u0418\7t\2\2"+
		"\u0394\u0395\7\u00d8\2\2\u0395\u0418\7\u008d\2\2\u0396\u0397\7\66\2\2"+
		"\u0397\u0418\7s\2\2\u0398\u0399\7N\2\2\u0399\u0418\7s\2\2\u039a\u039b"+
		"\7\20\2\2\u039b\u0418\7s\2\2\u039c\u039d\7\u008c\2\2\u039d\u0418\7\u00e2"+
		"\2\2\u039e\u039f\7\u008c\2\2\u039f\u0418\7?\2\2\u03a0\u03a1\7\u00f6\2"+
		"\2\u03a1\u0418\7\u00e2\2\2\u03a2\u03a3\7\u00f6\2\2\u03a3\u0418\7?\2\2"+
		"\u03a4\u03a5\7\66\2\2\u03a5\u03a6\7\u00e6\2\2\u03a6\u0418\7\u008f\2\2"+
		"\u03a7\u03a8\7N\2\2\u03a8\u03a9\7\u00e6\2\2\u03a9\u0418\7\u008f\2\2\u03aa"+
		"\u03ab\7\20\2\2\u03ab\u03ac\7\u00e2\2\2\u03ac\u03ad\5\u0090I\2\u03ad\u03ae"+
		"\7\u009c\2\2\u03ae\u03af\7(\2\2\u03af\u0418\3\2\2\2\u03b0\u03b1\7\20\2"+
		"\2\u03b1\u03b2\7\u00e2\2\2\u03b2\u03b3\5\u0090I\2\u03b3\u03b4\7(\2\2\u03b4"+
		"\u03b5\7\37\2\2\u03b5\u0418\3\2\2\2\u03b6\u03b7\7\20\2\2\u03b7\u03b8\7"+
		"\u00e2\2\2\u03b8\u03b9\5\u0090I\2\u03b9\u03ba\7\u009c\2\2\u03ba\u03bb"+
		"\7\u00dc\2\2\u03bb\u0418\3\2\2\2\u03bc\u03bd\7\20\2\2\u03bd\u03be\7\u00e2"+
		"\2\2\u03be\u03bf\5\u0090I\2\u03bf\u03c0\7\u00d9\2\2\u03c0\u03c1\7\37\2"+
		"\2\u03c1\u0418\3\2\2\2\u03c2\u03c3\7\20\2\2\u03c3\u03c4\7\u00e2\2\2\u03c4"+
		"\u03c5\5\u0090I\2\u03c5\u03c6\7\u009c\2\2\u03c6\u03c7\7\u00d9\2\2\u03c7"+
		"\u0418\3\2\2\2\u03c8\u03c9\7\20\2\2\u03c9\u03ca\7\u00e2\2\2\u03ca\u03cb"+
		"\5\u0090I\2\u03cb\u03cc\7\u009c\2\2\u03cc\u03cd\7\u00df\2\2\u03cd\u03ce"+
		"\7\27\2\2\u03ce\u03cf\7J\2\2\u03cf\u0418\3\2\2\2\u03d0\u03d1\7\20\2\2"+
		"\u03d1\u03d2\7\u00e2\2\2\u03d2\u03d3\5\u0090I\2\u03d3\u03d4\7\u00d5\2"+
		"\2\u03d4\u03d5\7\u00d9\2\2\u03d5\u03d6\7\u008b\2\2\u03d6\u0418\3\2\2\2"+
		"\u03d7\u03d8\7\20\2\2\u03d8\u03d9\7\u00e2\2\2\u03d9\u03da\5\u0090I\2\u03da"+
		"\u03db\7S\2\2\u03db\u03dc\7\u00ac\2\2\u03dc\u0418\3\2\2\2\u03dd\u03de"+
		"\7\20\2\2\u03de\u03df\7\u00e2\2\2\u03df\u03e0\5\u0090I\2\u03e0\u03e1\7"+
		"\25\2\2\u03e1\u03e2\7\u00ac\2\2\u03e2\u0418\3\2\2\2\u03e3\u03e4\7\20\2"+
		"\2\u03e4\u03e5\7\u00e2\2\2\u03e5\u03e6\5\u0090I\2\u03e6\u03e7\7\u00f1"+
		"\2\2\u03e7\u03e8\7\u00ac\2\2\u03e8\u0418\3\2\2\2\u03e9\u03ea\7\20\2\2"+
		"\u03ea\u03eb\7\u00e2\2\2\u03eb\u03ec\5\u0090I\2\u03ec\u03ed\7\u00ea\2"+
		"\2\u03ed\u0418\3\2\2\2\u03ee\u03ef\7\20\2\2\u03ef\u03f0\7\u00e2\2\2\u03f0"+
		"\u03f2\5\u0090I\2\u03f1\u03f3\5\"\22\2\u03f2\u03f1\3\2\2\2\u03f2\u03f3"+
		"\3\2\2\2\u03f3\u03f4\3\2\2\2\u03f4\u03f5\7\60\2\2\u03f5\u0418\3\2\2\2"+
		"\u03f6\u03f7\7\20\2\2\u03f7\u03f8\7\u00e2\2\2\u03f8\u03fa\5\u0090I\2\u03f9"+
		"\u03fb\5\"\22\2\u03fa\u03f9\3\2\2\2\u03fa\u03fb\3\2\2\2\u03fb\u03fc\3"+
		"\2\2\2\u03fc\u03fd\7\63\2\2\u03fd\u0418\3\2\2\2\u03fe\u03ff\7\20\2\2\u03ff"+
		"\u0400\7\u00e2\2\2\u0400\u0402\5\u0090I\2\u0401\u0403\5\"\22\2\u0402\u0401"+
		"\3\2\2\2\u0402\u0403\3\2\2\2\u0403\u0404\3\2\2\2\u0404\u0405\7\u00d5\2"+
		"\2\u0405\u0406\7]\2\2\u0406\u0418\3\2\2\2\u0407\u0408\7\20\2\2\u0408\u0409"+
		"\7\u00e2\2\2\u0409\u040b\5\u0090I\2\u040a\u040c\5\"\22\2\u040b\u040a\3"+
		"\2\2\2\u040b\u040c\3\2\2\2\u040c\u040d\3\2\2\2\u040d\u040e\7\u00c0\2\2"+
		"\u040e\u040f\7-\2\2\u040f\u0418\3\2\2\2\u0410\u0411\7\u00dd\2\2\u0411"+
		"\u0418\7\u00ec\2\2\u0412\u0418\7/\2\2\u0413\u0418\7\u00c8\2\2\u0414\u0418"+
		"\7I\2\2\u0415\u0416\7E\2\2\u0416\u0418\7d\2\2\u0417\u036d\3\2\2\2\u0417"+
		"\u036f\3\2\2\2\u0417\u0371\3\2\2\2\u0417\u0375\3\2\2\2\u0417\u0379\3\2"+
		"\2\2\u0417\u037b\3\2\2\2\u0417\u0380\3\2\2\2\u0417\u0382\3\2\2\2\u0417"+
		"\u0384\3\2\2\2\u0417\u0387\3\2\2\2\u0417\u0389\3\2\2\2\u0417\u038b\3\2"+
		"\2\2\u0417\u038d\3\2\2\2\u0417\u0390\3\2\2\2\u0417\u0392\3\2\2\2\u0417"+
		"\u0394\3\2\2\2\u0417\u0396\3\2\2\2\u0417\u0398\3\2\2\2\u0417\u039a\3\2"+
		"\2\2\u0417\u039c\3\2\2\2\u0417\u039e\3\2\2\2\u0417\u03a0\3\2\2\2\u0417"+
		"\u03a2\3\2\2\2\u0417\u03a4\3\2\2\2\u0417\u03a7\3\2\2\2\u0417\u03aa\3\2"+
		"\2\2\u0417\u03b0\3\2\2\2\u0417\u03b6\3\2\2\2\u0417\u03bc\3\2\2\2\u0417"+
		"\u03c2\3\2\2\2\u0417\u03c8\3\2\2\2\u0417\u03d0\3\2\2\2\u0417\u03d7\3\2"+
		"\2\2\u0417\u03dd\3\2\2\2\u0417\u03e3\3\2\2\2\u0417\u03e9\3\2\2\2\u0417"+
		"\u03ee\3\2\2\2\u0417\u03f6\3\2\2\2\u0417\u03fe\3\2\2\2\u0417\u0407\3\2"+
		"\2\2\u0417\u0410\3\2\2\2\u0417\u0412\3\2\2\2\u0417\u0413\3\2\2\2\u0417"+
		"\u0414\3\2\2\2\u0417\u0415\3\2\2\2\u0418\23\3\2\2\2\u0419\u041b\7\66\2"+
		"\2\u041a\u041c\7\u00e6\2\2\u041b\u041a\3\2\2\2\u041b\u041c\3\2\2\2\u041c"+
		"\u041e\3\2\2\2\u041d\u041f\7X\2\2\u041e\u041d\3\2\2\2\u041e\u041f\3\2"+
		"\2\2\u041f\u0420\3\2\2\2\u0420\u0424\7\u00e2\2\2\u0421\u0422\7o\2\2\u0422"+
		"\u0423\7\u009c\2\2\u0423\u0425\7T\2\2\u0424\u0421\3\2\2\2\u0424\u0425"+
		"\3\2\2\2\u0425\u0426\3\2\2\2\u0426\u0427\5\u008eH\2\u0427\25\3\2\2\2\u0428"+
		"\u0429\7(\2\2\u0429\u042a\7\37\2\2\u042a\u042e\5x=\2\u042b\u042c\7\u00dc"+
		"\2\2\u042c\u042d\7\37\2\2\u042d\u042f\5|?\2\u042e\u042b\3\2\2\2\u042e"+
		"\u042f\3\2\2\2\u042f\u0430\3\2\2\2\u0430\u0431\7{\2\2\u0431\u0432\7\u011c"+
		"\2\2\u0432\u0433\7\36\2\2\u0433\27\3\2\2\2\u0434\u0435\7\u00d9\2\2\u0435"+
		"\u0436\7\37\2\2\u0436\u0437\5x=\2\u0437\u043a\7\u00a0\2\2\u0438\u043b"+
		"\5:\36\2\u0439\u043b\5<\37\2\u043a\u0438\3\2\2\2\u043a\u0439\3\2\2\2\u043b"+
		"\u043f\3\2\2\2\u043c\u043d\7\u00df\2\2\u043d\u043e\7\27\2\2\u043e\u0440"+
		"\7J\2\2\u043f\u043c\3\2\2\2\u043f\u0440\3\2\2\2\u0440\31\3\2\2\2\u0441"+
		"\u0442\7\u008b\2\2\u0442\u0443\7\u0118\2\2\u0443\33\3\2\2\2\u0444\u0446"+
		"\5,\27\2\u0445\u0444\3\2\2\2\u0445\u0446\3\2\2\2\u0446\u0447\3\2\2\2\u0447"+
		"\u0448\5H%\2\u0448\35\3\2\2\2\u0449\u044a\7x\2\2\u044a\u044b\7\u00ab\2"+
		"\2\u044b\u044c\7\u00e2\2\2\u044c\u0453\5\u0090I\2\u044d\u0451\5\"\22\2"+
		"\u044e\u044f\7o\2\2\u044f\u0450\7\u009c\2\2\u0450\u0452\7T\2\2\u0451\u044e"+
		"\3\2\2\2\u0451\u0452\3\2\2\2\u0452\u0454\3\2\2\2\u0453\u044d\3\2\2\2\u0453"+
		"\u0454\3\2\2\2\u0454\u047a\3\2\2\2\u0455\u0456\7x\2\2\u0456\u0458\7{\2"+
		"\2\u0457\u0459\7\u00e2\2\2\u0458\u0457\3\2\2\2\u0458\u0459\3\2\2\2\u0459"+
		"\u045a\3\2\2\2\u045a\u045c\5\u0090I\2\u045b\u045d\5\"\22\2\u045c\u045b"+
		"\3\2\2\2\u045c\u045d\3\2\2\2\u045d\u047a\3\2\2\2\u045e\u045f\7x\2\2\u045f"+
		"\u0461\7\u00ab\2\2\u0460\u0462\7\u008a\2\2\u0461\u0460\3\2\2\2\u0461\u0462"+
		"\3\2\2\2\u0462\u0463\3\2\2\2\u0463\u0464\7K\2\2\u0464\u0466\7\u0118\2"+
		"\2\u0465\u0467\5\u008cG\2\u0466\u0465\3\2\2\2\u0466\u0467\3\2\2\2\u0467"+
		"\u0469\3\2\2\2\u0468\u046a\5> \2\u0469\u0468\3\2\2\2\u0469\u046a\3\2\2"+
		"\2\u046a\u047a\3\2\2\2\u046b\u046c\7x\2\2\u046c\u046e\7\u00ab\2\2\u046d"+
		"\u046f\7\u008a\2\2\u046e\u046d\3\2\2\2\u046e\u046f\3\2\2\2\u046f\u0470"+
		"\3\2\2\2\u0470\u0472\7K\2\2\u0471\u0473\7\u0118\2\2\u0472\u0471\3\2\2"+
		"\2\u0472\u0473\3\2\2\2\u0473\u0474\3\2\2\2\u0474\u0477\5\60\31\2\u0475"+
		"\u0476\7\u00a3\2\2\u0476\u0478\5\62\32\2\u0477\u0475\3\2\2\2\u0477\u0478"+
		"\3\2\2\2\u0478\u047a\3\2\2\2\u0479\u0449\3\2\2\2\u0479\u0455\3\2\2\2\u0479"+
		"\u045e\3\2\2\2\u0479\u046b\3\2\2\2\u047a\37\3\2\2\2\u047b\u047d\5\"\22"+
		"\2\u047c\u047e\5\32\16\2\u047d\u047c\3\2\2\2\u047d\u047e\3\2\2\2\u047e"+
		"!\3\2\2\2\u047f\u0480\7\u00ac\2\2\u0480\u0481\7\3\2\2\u0481\u0486\5$\23"+
		"\2\u0482\u0483\7\5\2\2\u0483\u0485\5$\23\2\u0484\u0482\3\2\2\2\u0485\u0488"+
		"\3\2\2\2\u0486\u0484\3\2\2\2\u0486\u0487\3\2\2\2\u0487\u0489\3\2\2\2\u0488"+
		"\u0486\3\2\2\2\u0489\u048a\7\4\2\2\u048a#\3\2\2\2\u048b\u048e\5\u00d4"+
		"k\2\u048c\u048d\7\u0105\2\2\u048d\u048f\5\u00a8U\2\u048e\u048c\3\2\2\2"+
		"\u048e\u048f\3\2\2\2\u048f%\3\2\2\2\u0490\u0491\t\t\2\2\u0491\'\3\2\2"+
		"\2\u0492\u0498\5\u00d2j\2\u0493\u0498\7\u0118\2\2\u0494\u0498\5\u00aa"+
		"V\2\u0495\u0498\5\u00acW\2\u0496\u0498\5\u00aeX\2\u0497\u0492\3\2\2\2"+
		"\u0497\u0493\3\2\2\2\u0497\u0494\3\2\2\2\u0497\u0495\3\2\2\2\u0497\u0496"+
		"\3\2\2\2\u0498)\3\2\2\2\u0499\u049e\5\u00d4k\2\u049a\u049b\7\6\2\2\u049b"+
		"\u049d\5\u00d4k\2\u049c\u049a\3\2\2\2\u049d\u04a0\3\2\2\2\u049e\u049c"+
		"\3\2\2\2\u049e\u049f\3\2\2\2\u049f+\3\2\2\2\u04a0\u049e\3\2\2\2\u04a1"+
		"\u04a2\7\u0102\2\2\u04a2\u04a7\5.\30\2\u04a3\u04a4\7\5\2\2\u04a4\u04a6"+
		"\5.\30\2\u04a5\u04a3\3\2\2\2\u04a6\u04a9\3\2\2\2\u04a7\u04a5\3\2\2\2\u04a7"+
		"\u04a8\3\2\2\2\u04a8-\3\2\2\2\u04a9\u04a7\3\2\2\2\u04aa\u04ac\5\u00d4"+
		"k\2\u04ab\u04ad\7\27\2\2\u04ac\u04ab\3\2\2\2\u04ac\u04ad\3\2\2\2\u04ad"+
		"\u04ae\3\2\2\2\u04ae\u04af\7\3\2\2\u04af\u04b0\5\34\17\2\u04b0\u04b1\7"+
		"\4\2\2\u04b1/\3\2\2\2\u04b2\u04b3\7\u00fa\2\2\u04b3\u04b4\5\u00d2j\2\u04b4"+
		"\61\3\2\2\2\u04b5\u04b6\7\3\2\2\u04b6\u04bb\5\64\33\2\u04b7\u04b8\7\5"+
		"\2\2\u04b8\u04ba\5\64\33\2\u04b9\u04b7\3\2\2\2\u04ba\u04bd\3\2\2\2\u04bb"+
		"\u04b9\3\2\2\2\u04bb\u04bc\3\2\2\2\u04bc\u04be\3\2\2\2\u04bd\u04bb\3\2"+
		"\2\2\u04be\u04bf\7\4\2\2\u04bf\63\3\2\2\2\u04c0\u04c5\5\66\34\2\u04c1"+
		"\u04c3\7\u0105\2\2\u04c2\u04c1\3\2\2\2\u04c2\u04c3\3\2\2\2\u04c3\u04c4"+
		"\3\2\2\2\u04c4\u04c6\58\35\2\u04c5\u04c2\3\2\2\2\u04c5\u04c6\3\2\2\2\u04c6"+
		"\65\3\2\2\2\u04c7\u04cc\5\u00d4k\2\u04c8\u04c9\7\6\2\2\u04c9\u04cb\5\u00d4"+
		"k\2\u04ca\u04c8\3\2\2\2\u04cb\u04ce\3\2\2\2\u04cc\u04ca\3\2\2\2\u04cc"+
		"\u04cd\3\2\2\2\u04cd\u04d1\3\2\2\2\u04ce\u04cc\3\2\2\2\u04cf\u04d1\7\u0118"+
		"\2\2\u04d0\u04c7\3\2\2\2\u04d0\u04cf\3\2\2\2\u04d1\67\3\2\2\2\u04d2\u04d7"+
		"\7\u011c\2\2\u04d3\u04d7\7\u011d\2\2\u04d4\u04d7\5\u00b0Y\2\u04d5\u04d7"+
		"\7\u0118\2\2\u04d6\u04d2\3\2\2\2\u04d6\u04d3\3\2\2\2\u04d6\u04d4\3\2\2"+
		"\2\u04d6\u04d5\3\2\2\2\u04d79\3\2\2\2\u04d8\u04d9\7\3\2\2\u04d9\u04de"+
		"\5\u00a8U\2\u04da\u04db\7\5\2\2\u04db\u04dd\5\u00a8U\2\u04dc\u04da\3\2"+
		"\2\2\u04dd\u04e0\3\2\2\2\u04de\u04dc\3\2\2\2\u04de\u04df\3\2\2\2\u04df"+
		"\u04e1\3\2\2\2\u04e0\u04de\3\2\2\2\u04e1\u04e2\7\4\2\2\u04e2;\3\2\2\2"+
		"\u04e3\u04e4\7\3\2\2\u04e4\u04e9\5:\36\2\u04e5\u04e6\7\5\2\2\u04e6\u04e8"+
		"\5:\36\2\u04e7\u04e5\3\2\2\2\u04e8\u04eb\3\2\2\2\u04e9\u04e7\3\2\2\2\u04e9"+
		"\u04ea\3\2\2\2\u04ea\u04ec\3\2\2\2\u04eb\u04e9\3\2\2\2\u04ec\u04ed\7\4"+
		"\2\2\u04ed=\3\2\2\2\u04ee\u04ef\7\u00df\2\2\u04ef\u04f0\7\27\2\2\u04f0"+
		"\u04f5\5@!\2\u04f1\u04f2\7\u00df\2\2\u04f2\u04f3\7\37\2\2\u04f3\u04f5"+
		"\5B\"\2\u04f4\u04ee\3\2\2\2\u04f4\u04f1\3\2\2\2\u04f5?\3\2\2\2\u04f6\u04f7"+
		"\7w\2\2\u04f7\u04f8\7\u0118\2\2\u04f8\u04f9\7\u00a8\2\2\u04f9\u04fc\7"+
		"\u0118\2\2\u04fa\u04fc\5\u00d4k\2\u04fb\u04f6\3\2\2\2\u04fb\u04fa\3\2"+
		"\2\2\u04fcA\3\2\2\2\u04fd\u0501\7\u0118\2\2\u04fe\u04ff\7\u0102\2\2\u04ff"+
		"\u0500\7\u00d3\2\2\u0500\u0502\5\62\32\2\u0501\u04fe\3\2\2\2\u0501\u0502"+
		"\3\2\2\2\u0502C\3\2\2\2\u0503\u0504\5\u00d4k\2\u0504\u0505\7\u0118\2\2"+
		"\u0505E\3\2\2\2\u0506\u0507\5\36\20\2\u0507\u0508\5P)\2\u0508\u0509\5"+
		"J&\2\u0509\u0511\3\2\2\2\u050a\u050c\5\\/\2\u050b\u050d\5L\'\2\u050c\u050b"+
		"\3\2\2\2\u050d\u050e\3\2\2\2\u050e\u050c\3\2\2\2\u050e\u050f\3\2\2\2\u050f"+
		"\u0511\3\2\2\2\u0510\u0506\3\2\2\2\u0510\u050a\3\2\2\2\u0511G\3\2\2\2"+
		"\u0512\u0513\5P)\2\u0513\u0514\5J&\2\u0514\u051c\3\2\2\2\u0515\u0517\5"+
		"\\/\2\u0516\u0518\5N(\2\u0517\u0516\3\2\2\2\u0518\u0519\3\2\2\2\u0519"+
		"\u0517\3\2\2\2\u0519\u051a\3\2\2\2\u051a\u051c\3\2\2\2\u051b\u0512\3\2"+
		"\2\2\u051b\u0515\3\2\2\2\u051cI\3\2\2\2\u051d\u051e\7\u00a5\2\2\u051e"+
		"\u051f\7\37\2\2\u051f\u0524\5T+\2\u0520\u0521\7\5\2\2\u0521\u0523\5T+"+
		"\2\u0522\u0520\3\2\2\2\u0523\u0526\3\2\2\2\u0524\u0522\3\2\2\2\u0524\u0525"+
		"\3\2\2\2\u0525\u0528\3\2\2\2\u0526\u0524\3\2\2\2\u0527\u051d\3\2\2\2\u0527"+
		"\u0528\3\2\2\2\u0528\u0533\3\2\2\2\u0529\u052a\7\'\2\2\u052a\u052b\7\37"+
		"\2\2\u052b\u0530\5\u009eP\2\u052c\u052d\7\5\2\2\u052d\u052f\5\u009eP\2"+
		"\u052e\u052c\3\2\2\2\u052f\u0532\3\2\2\2\u0530\u052e\3\2\2\2\u0530\u0531"+
		"\3\2\2\2\u0531\u0534\3\2\2\2\u0532\u0530\3\2\2\2\u0533\u0529\3\2\2\2\u0533"+
		"\u0534\3\2\2\2\u0534\u053f\3\2\2\2\u0535\u0536\7M\2\2\u0536\u0537\7\37"+
		"\2\2\u0537\u053c\5\u009eP\2\u0538\u0539\7\5\2\2\u0539\u053b\5\u009eP\2"+
		"\u053a\u0538\3\2\2\2\u053b\u053e\3\2\2\2\u053c\u053a\3\2\2\2\u053c\u053d"+
		"\3\2\2\2\u053d\u0540\3\2\2\2\u053e\u053c\3\2\2\2\u053f\u0535\3\2\2\2\u053f"+
		"\u0540\3\2\2\2\u0540\u054b\3\2\2\2\u0541\u0542\7\u00db\2\2\u0542\u0543"+
		"\7\37\2\2\u0543\u0548\5T+\2\u0544\u0545\7\5\2\2\u0545\u0547\5T+\2\u0546"+
		"\u0544\3\2\2\2\u0547\u054a\3\2\2\2\u0548\u0546\3\2\2\2\u0548\u0549\3\2"+
		"\2\2\u0549\u054c\3\2\2\2\u054a\u0548\3\2\2\2\u054b\u0541\3\2\2\2\u054b"+
		"\u054c\3\2\2\2\u054c\u054e\3\2\2\2\u054d\u054f\5\u00c8e\2\u054e\u054d"+
		"\3\2\2\2\u054e\u054f\3\2\2\2\u054f\u0555\3\2\2\2\u0550\u0553\7\u0086\2"+
		"\2\u0551\u0554\7\17\2\2\u0552\u0554\5\u009eP\2\u0553\u0551\3\2\2\2\u0553"+
		"\u0552\3\2\2\2\u0554\u0556\3\2\2\2\u0555\u0550\3\2\2\2\u0555\u0556\3\2"+
		"\2\2\u0556K\3\2\2\2\u0557\u0558\5\36\20\2\u0558\u0559\5N(\2\u0559M\3\2"+
		"\2\2\u055a\u055b\5V,\2\u055b\u055c\5J&\2\u055cO\3\2\2\2\u055d\u055e\b"+
		")\1\2\u055e\u055f\5R*\2\u055f\u0577\3\2\2\2\u0560\u0561\f\5\2\2\u0561"+
		"\u0562\6)\3\2\u0562\u0564\t\n\2\2\u0563\u0565\5j\66\2\u0564\u0563\3\2"+
		"\2\2\u0564\u0565\3\2\2\2\u0565\u0566\3\2\2\2\u0566\u0576\5P)\6\u0567\u0568"+
		"\f\4\2\2\u0568\u0569\6)\5\2\u0569\u056b\7y\2\2\u056a\u056c\5j\66\2\u056b"+
		"\u056a\3\2\2\2\u056b\u056c\3\2\2\2\u056c\u056d\3\2\2\2\u056d\u0576\5P"+
		")\5\u056e\u056f\f\3\2\2\u056f\u0570\6)\7\2\u0570\u0572\t\13\2\2\u0571"+
		"\u0573\5j\66\2\u0572\u0571\3\2\2\2\u0572\u0573\3\2\2\2\u0573\u0574\3\2"+
		"\2\2\u0574\u0576\5P)\4\u0575\u0560\3\2\2\2\u0575\u0567\3\2\2\2\u0575\u056e"+
		"\3\2\2\2\u0576\u0579\3\2\2\2\u0577\u0575\3\2\2\2\u0577\u0578\3\2\2\2\u0578"+
		"Q\3\2\2\2\u0579\u0577\3\2\2\2\u057a\u0583\5V,\2\u057b\u057c\7\u00e2\2"+
		"\2\u057c\u0583\5\u0090I\2\u057d\u0583\5\u0086D\2\u057e\u057f\7\3\2\2\u057f"+
		"\u0580\5H%\2\u0580\u0581\7\4\2\2\u0581\u0583\3\2\2\2\u0582\u057a\3\2\2"+
		"\2\u0582\u057b\3\2\2\2\u0582\u057d\3\2\2\2\u0582\u057e\3\2\2\2\u0583S"+
		"\3\2\2\2\u0584\u0586\5\u009eP\2\u0585\u0587\t\f\2\2\u0586\u0585\3\2\2"+
		"\2\u0586\u0587\3\2\2\2\u0587\u058a\3\2\2\2\u0588\u0589\7\u009e\2\2\u0589"+
		"\u058b\t\r\2\2\u058a\u0588\3\2\2\2\u058a\u058b\3\2\2\2\u058bU\3\2\2\2"+
		"\u058c\u058d\7\u00cf\2\2\u058d\u058e\7\u00ee\2\2\u058e\u058f\7\3\2\2\u058f"+
		"\u0590\5\u0096L\2\u0590\u0591\7\4\2\2\u0591\u0597\3\2\2\2\u0592\u0593"+
		"\7\u0090\2\2\u0593\u0597\5\u0096L\2\u0594\u0595\7\u00bb\2\2\u0595\u0597"+
		"\5\u0096L\2\u0596\u058c\3\2\2\2\u0596\u0592\3\2\2\2\u0596\u0594\3\2\2"+
		"\2\u0597\u0599\3\2\2\2\u0598\u059a\5\u008cG\2\u0599\u0598\3\2\2\2\u0599"+
		"\u059a\3\2\2\2\u059a\u059d\3\2\2\2\u059b\u059c\7\u00b9\2\2\u059c\u059e"+
		"\7\u0118\2\2\u059d\u059b\3\2\2\2\u059d\u059e\3\2\2\2\u059e\u059f\3\2\2"+
		"\2\u059f\u05a0\7\u00fa\2\2\u05a0\u05ad\7\u0118\2\2\u05a1\u05ab\7\27\2"+
		"\2\u05a2\u05ac\5z>\2\u05a3\u05ac\5\u00be`\2\u05a4\u05a7\7\3\2\2\u05a5"+
		"\u05a8\5z>\2\u05a6\u05a8\5\u00be`\2\u05a7\u05a5\3\2\2\2\u05a7\u05a6\3"+
		"\2\2\2\u05a8\u05a9\3\2\2\2\u05a9\u05aa\7\4\2\2\u05aa\u05ac\3\2\2\2\u05ab"+
		"\u05a2\3\2\2\2\u05ab\u05a3\3\2\2\2\u05ab\u05a4\3\2\2\2\u05ac\u05ae\3\2"+
		"\2\2\u05ad\u05a1\3\2\2\2\u05ad\u05ae\3\2\2\2\u05ae\u05b0\3\2\2\2\u05af"+
		"\u05b1\5\u008cG\2\u05b0\u05af\3\2\2\2\u05b0\u05b1\3\2\2\2\u05b1\u05b4"+
		"\3\2\2\2\u05b2\u05b3\7\u00b8\2\2\u05b3\u05b5\7\u0118\2\2\u05b4\u05b2\3"+
		"\2\2\2\u05b4\u05b5\3\2\2\2\u05b5\u05b7\3\2\2\2\u05b6\u05b8\5\\/\2\u05b7"+
		"\u05b6\3\2\2\2\u05b7\u05b8\3\2\2\2\u05b8\u05bb\3\2\2\2\u05b9\u05ba\7\u0100"+
		"\2\2\u05ba\u05bc\5\u00a0Q\2\u05bb\u05b9\3\2\2\2\u05bb\u05bc\3\2\2\2\u05bc"+
		"\u05ea\3\2\2\2\u05bd\u05c1\7\u00cf\2\2\u05be\u05c0\5X-\2\u05bf\u05be\3"+
		"\2\2\2\u05c0\u05c3\3\2\2\2\u05c1\u05bf\3\2\2\2\u05c1\u05c2\3\2\2\2\u05c2"+
		"\u05c5\3\2\2\2\u05c3\u05c1\3\2\2\2\u05c4\u05c6\5j\66\2\u05c5\u05c4\3\2"+
		"\2\2\u05c5\u05c6\3\2\2\2\u05c6\u05c7\3\2\2\2\u05c7\u05c9\5\u0096L\2\u05c8"+
		"\u05ca\5\\/\2\u05c9\u05c8\3\2\2\2\u05c9\u05ca\3\2\2\2\u05ca\u05d4\3\2"+
		"\2\2\u05cb\u05d1\5\\/\2\u05cc\u05ce\7\u00cf\2\2\u05cd\u05cf\5j\66\2\u05ce"+
		"\u05cd\3\2\2\2\u05ce\u05cf\3\2\2\2\u05cf\u05d0\3\2\2\2\u05d0\u05d2\5\u0096"+
		"L\2\u05d1\u05cc\3\2\2\2\u05d1\u05d2\3\2\2\2\u05d2\u05d4\3\2\2\2\u05d3"+
		"\u05bd\3\2\2\2\u05d3\u05cb\3\2\2\2\u05d4\u05d8\3\2\2\2\u05d5\u05d7\5h"+
		"\65\2\u05d6\u05d5\3\2\2\2\u05d7\u05da\3\2\2\2\u05d8\u05d6\3\2\2\2\u05d8"+
		"\u05d9\3\2\2\2\u05d9\u05dd\3\2\2\2\u05da\u05d8\3\2\2\2\u05db\u05dc\7\u0100"+
		"\2\2\u05dc\u05de\5\u00a0Q\2\u05dd\u05db\3\2\2\2\u05dd\u05de\3\2\2\2\u05de"+
		"\u05e0\3\2\2\2\u05df\u05e1\5^\60\2\u05e0\u05df\3\2\2\2\u05e0\u05e1\3\2"+
		"\2\2\u05e1\u05e4\3\2\2\2\u05e2\u05e3\7l\2\2\u05e3\u05e5\5\u00a0Q\2\u05e4"+
		"\u05e2\3\2\2\2\u05e4\u05e5\3\2\2\2\u05e5\u05e7\3\2\2\2\u05e6\u05e8\5\u00c8"+
		"e\2\u05e7\u05e6\3\2\2\2\u05e7\u05e8\3\2\2\2\u05e8\u05ea\3\2\2\2\u05e9"+
		"\u0596\3\2\2\2\u05e9\u05d3\3\2\2\2\u05eaW\3\2\2\2\u05eb\u05ec\7\7\2\2"+
		"\u05ec\u05f3\5Z.\2\u05ed\u05ef\7\5\2\2\u05ee\u05ed\3\2\2\2\u05ee\u05ef"+
		"\3\2\2\2\u05ef\u05f0\3\2\2\2\u05f0\u05f2\5Z.\2\u05f1\u05ee\3\2\2\2\u05f2"+
		"\u05f5\3\2\2\2\u05f3\u05f1\3\2\2\2\u05f3\u05f4\3\2\2\2\u05f4\u05f6\3\2"+
		"\2\2\u05f5\u05f3\3\2\2\2\u05f6\u05f7\7\b\2\2\u05f7Y\3\2\2\2\u05f8\u0606"+
		"\5\u00d4k\2\u05f9\u05fa\5\u00d4k\2\u05fa\u05fb\7\3\2\2\u05fb\u0600\5\u00a6"+
		"T\2\u05fc\u05fd\7\5\2\2\u05fd\u05ff\5\u00a6T\2\u05fe\u05fc\3\2\2\2\u05ff"+
		"\u0602\3\2\2\2\u0600\u05fe\3\2\2\2\u0600\u0601\3\2\2\2\u0601\u0603\3\2"+
		"\2\2\u0602\u0600\3\2\2\2\u0603\u0604\7\4\2\2\u0604\u0606\3\2\2\2\u0605"+
		"\u05f8\3\2\2\2\u0605\u05f9\3\2\2\2\u0606[\3\2\2\2\u0607\u0608\7d\2\2\u0608"+
		"\u060d\5l\67\2\u0609\u060a\7\5\2\2\u060a\u060c\5l\67\2\u060b\u0609\3\2"+
		"\2\2\u060c\u060f\3\2\2\2\u060d\u060b\3\2\2\2\u060d\u060e\3\2\2\2\u060e"+
		"\u0613\3\2\2\2\u060f\u060d\3\2\2\2\u0610\u0612\5h\65\2\u0611\u0610\3\2"+
		"\2\2\u0612\u0615\3\2\2\2\u0613\u0611\3\2\2\2\u0613\u0614\3\2\2\2\u0614"+
		"\u0617\3\2\2\2\u0615\u0613\3\2\2\2\u0616\u0618\5b\62\2\u0617\u0616\3\2"+
		"\2\2\u0617\u0618\3\2\2\2\u0618]\3\2\2\2\u0619\u061a\7j\2\2\u061a\u061b"+
		"\7\37\2\2\u061b\u0620\5\u009eP\2\u061c\u061d\7\5\2\2\u061d\u061f\5\u009e"+
		"P\2\u061e\u061c\3\2\2\2\u061f\u0622\3\2\2\2\u0620\u061e\3\2\2\2\u0620"+
		"\u0621\3\2\2\2\u0621\u0634\3\2\2\2\u0622\u0620\3\2\2\2\u0623\u0624\7\u0102"+
		"\2\2\u0624\u0635\7\u00c9\2\2\u0625\u0626\7\u0102\2\2\u0626\u0635\78\2"+
		"\2\u0627\u0628\7k\2\2\u0628\u0629\7\u00d7\2\2\u0629\u062a\7\3\2\2\u062a"+
		"\u062f\5`\61\2\u062b\u062c\7\5\2\2\u062c\u062e\5`\61\2\u062d\u062b\3\2"+
		"\2\2\u062e\u0631\3\2\2\2\u062f\u062d\3\2\2\2\u062f\u0630\3\2\2\2\u0630"+
		"\u0632\3\2\2\2\u0631\u062f\3\2\2\2\u0632\u0633\7\4\2\2\u0633\u0635\3\2"+
		"\2\2\u0634\u0623\3\2\2\2\u0634\u0625\3\2\2\2\u0634\u0627\3\2\2\2\u0634"+
		"\u0635\3\2\2\2\u0635\u0646\3\2\2\2\u0636\u0637\7j\2\2\u0637\u0638\7\37"+
		"\2\2\u0638\u0639\7k\2\2\u0639\u063a\7\u00d7\2\2\u063a\u063b\7\3\2\2\u063b"+
		"\u0640\5`\61\2\u063c\u063d\7\5\2\2\u063d\u063f\5`\61\2\u063e\u063c\3\2"+
		"\2\2\u063f\u0642\3\2\2\2\u0640\u063e\3\2\2\2\u0640\u0641\3\2\2\2\u0641"+
		"\u0643\3\2\2\2\u0642\u0640\3\2\2\2\u0643\u0644\7\4\2\2\u0644\u0646\3\2"+
		"\2\2\u0645\u0619\3\2\2\2\u0645\u0636\3\2\2\2\u0646_\3\2\2\2\u0647\u0650"+
		"\7\3\2\2\u0648\u064d\5\u009eP\2\u0649\u064a\7\5\2\2\u064a\u064c\5\u009e"+
		"P\2\u064b\u0649\3\2\2\2\u064c\u064f\3\2\2\2\u064d\u064b\3\2\2\2\u064d"+
		"\u064e\3\2\2\2\u064e\u0651\3\2\2\2\u064f\u064d\3\2\2\2\u0650\u0648\3\2"+
		"\2\2\u0650\u0651\3\2\2\2\u0651\u0652\3\2\2\2\u0652\u0655\7\4\2\2\u0653"+
		"\u0655\5\u009eP\2\u0654\u0647\3\2\2\2\u0654\u0653\3\2\2\2\u0655a\3\2\2"+
		"\2\u0656\u0657\7\u00b0\2\2\u0657\u0658\7\3\2\2\u0658\u0659\5\u0096L\2"+
		"\u0659\u065a\7`\2\2\u065a\u065b\5d\63\2\u065b\u065c\7r\2\2\u065c\u065d"+
		"\7\3\2\2\u065d\u0662\5f\64\2\u065e\u065f\7\5\2\2\u065f\u0661\5f\64\2\u0660"+
		"\u065e\3\2\2\2\u0661\u0664\3\2\2\2\u0662\u0660\3\2\2\2\u0662\u0663\3\2"+
		"\2\2\u0663\u0665\3\2\2\2\u0664\u0662\3\2\2\2\u0665\u0666\7\4\2\2\u0666"+
		"\u0667\7\4\2\2\u0667c\3\2\2\2\u0668\u0675\5\u00d4k\2\u0669\u066a\7\3\2"+
		"\2\u066a\u066f\5\u00d4k\2\u066b\u066c\7\5\2\2\u066c\u066e\5\u00d4k\2\u066d"+
		"\u066b\3\2\2\2\u066e\u0671\3\2\2\2\u066f\u066d\3\2\2\2\u066f\u0670\3\2"+
		"\2\2\u0670\u0672\3\2\2\2\u0671\u066f\3\2\2\2\u0672\u0673\7\4\2\2\u0673"+
		"\u0675\3\2\2\2\u0674\u0668\3\2\2\2\u0674\u0669\3\2\2\2\u0675e\3\2\2\2"+
		"\u0676\u067b\5\u009eP\2\u0677\u0679\7\27\2\2\u0678\u0677\3\2\2\2\u0678"+
		"\u0679\3\2\2\2\u0679\u067a\3\2\2\2\u067a\u067c\5\u00d4k\2\u067b\u0678"+
		"\3\2\2\2\u067b\u067c\3\2\2\2\u067cg\3\2\2\2\u067d\u067e\7\u0081\2\2\u067e"+
		"\u0680\7\u00fc\2\2\u067f\u0681\7\u00a7\2\2\u0680\u067f\3\2\2\2\u0680\u0681"+
		"\3\2\2\2\u0681\u0682\3\2\2\2\u0682\u0683\5\u00d2j\2\u0683\u068c\7\3\2"+
		"\2\u0684\u0689\5\u009eP\2\u0685\u0686\7\5\2\2\u0686\u0688\5\u009eP\2\u0687"+
		"\u0685\3\2\2\2\u0688\u068b\3\2\2\2\u0689\u0687\3\2\2\2\u0689\u068a\3\2"+
		"\2\2\u068a\u068d\3\2\2\2\u068b\u0689\3\2\2\2\u068c\u0684\3\2\2\2\u068c"+
		"\u068d\3\2\2\2\u068d\u068e\3\2\2\2\u068e\u068f\7\4\2\2\u068f\u069b\5\u00d4"+
		"k\2\u0690\u0692\7\27\2\2\u0691\u0690\3\2\2\2\u0691\u0692\3\2\2\2\u0692"+
		"\u0693\3\2\2\2\u0693\u0698\5\u00d4k\2\u0694\u0695\7\5\2\2\u0695\u0697"+
		"\5\u00d4k\2\u0696\u0694\3\2\2\2\u0697\u069a\3\2\2\2\u0698\u0696\3\2\2"+
		"\2\u0698\u0699\3\2\2\2\u0699\u069c\3\2\2\2\u069a\u0698\3\2\2\2\u069b\u0691"+
		"\3\2\2\2\u069b\u069c\3\2\2\2\u069ci\3\2\2\2\u069d\u069e\t\16\2\2\u069e"+
		"k\3\2\2\2\u069f\u06a3\5\u0084C\2\u06a0\u06a2\5n8\2\u06a1\u06a0\3\2\2\2"+
		"\u06a2\u06a5\3\2\2\2\u06a3\u06a1\3\2\2\2\u06a3\u06a4\3\2\2\2\u06a4m\3"+
		"\2\2\2\u06a5\u06a3\3\2\2\2\u06a6\u06a7\5p9\2\u06a7\u06a8\7~\2\2\u06a8"+
		"\u06aa\5\u0084C\2\u06a9\u06ab\5r:\2\u06aa\u06a9\3\2\2\2\u06aa\u06ab\3"+
		"\2\2\2\u06ab\u06b2\3\2\2\2\u06ac\u06ad\7\u009a\2\2\u06ad\u06ae\5p9\2\u06ae"+
		"\u06af\7~\2\2\u06af\u06b0\5\u0084C\2\u06b0\u06b2\3\2\2\2\u06b1\u06a6\3"+
		"\2\2\2\u06b1\u06ac\3\2\2\2\u06b2o\3\2\2\2\u06b3\u06b5\7u\2\2\u06b4\u06b3"+
		"\3\2\2\2\u06b4\u06b5\3\2\2\2\u06b5\u06cc\3\2\2\2\u06b6\u06cc\7\67\2\2"+
		"\u06b7\u06b9\7\u0084\2\2\u06b8\u06ba\7\u00a7\2\2\u06b9\u06b8\3\2\2\2\u06b9"+
		"\u06ba\3\2\2\2\u06ba\u06cc\3\2\2\2\u06bb\u06bd\7\u0084\2\2\u06bc\u06bb"+
		"\3\2\2\2\u06bc\u06bd\3\2\2\2\u06bd\u06be\3\2\2\2\u06be\u06cc\7\u00d0\2"+
		"\2\u06bf\u06c1\7\u00c4\2\2\u06c0\u06c2\7\u00a7\2\2\u06c1\u06c0\3\2\2\2"+
		"\u06c1\u06c2\3\2\2\2\u06c2\u06cc\3\2\2\2\u06c3\u06c5\7e\2\2\u06c4\u06c6"+
		"\7\u00a7\2\2\u06c5\u06c4\3\2\2\2\u06c5\u06c6\3\2\2\2\u06c6\u06cc\3\2\2"+
		"\2\u06c7\u06c9\7\u0084\2\2\u06c8\u06c7\3\2\2\2\u06c8\u06c9\3\2\2\2\u06c9"+
		"\u06ca\3\2\2\2\u06ca\u06cc\7\23\2\2\u06cb\u06b4\3\2\2\2\u06cb\u06b6\3"+
		"\2\2\2\u06cb\u06b7\3\2\2\2\u06cb\u06bc\3\2\2\2\u06cb\u06bf\3\2\2\2\u06cb"+
		"\u06c3\3\2\2\2\u06cb\u06c8\3\2\2\2\u06ccq\3\2\2\2\u06cd\u06ce\7\u00a0"+
		"\2\2\u06ce\u06d2\5\u00a0Q\2\u06cf\u06d0\7\u00fa\2\2\u06d0\u06d2\5x=\2"+
		"\u06d1\u06cd\3\2\2\2\u06d1\u06cf\3\2\2\2\u06d2s\3\2\2\2\u06d3\u06d4\7"+
		"\u00e4\2\2\u06d4\u06d6\7\3\2\2\u06d5\u06d7\5v<\2\u06d6\u06d5\3\2\2\2\u06d6"+
		"\u06d7\3\2\2\2\u06d7\u06d8\3\2\2\2\u06d8\u06d9\7\4\2\2\u06d9u\3\2\2\2"+
		"\u06da\u06dc\7\u010e\2\2\u06db\u06da\3\2\2\2\u06db\u06dc\3\2\2\2\u06dc"+
		"\u06dd\3\2\2\2\u06dd\u06de\t\17\2\2\u06de\u06f3\7\u00af\2\2\u06df\u06e0"+
		"\5\u009eP\2\u06e0\u06e1\7\u00cb\2\2\u06e1\u06f3\3\2\2\2\u06e2\u06e3\7"+
		"\35\2\2\u06e3\u06e4\7\u011c\2\2\u06e4\u06e5\7\u00a6\2\2\u06e5\u06e6\7"+
		"\u009f\2\2\u06e6\u06ef\7\u011c\2\2\u06e7\u06ed\7\u00a0\2\2\u06e8\u06ee"+
		"\5\u00d4k\2\u06e9\u06ea\5\u00d2j\2\u06ea\u06eb\7\3\2\2\u06eb\u06ec\7\4"+
		"\2\2\u06ec\u06ee\3\2\2\2\u06ed\u06e8\3\2\2\2\u06ed\u06e9\3\2\2\2\u06ee"+
		"\u06f0\3\2\2\2\u06ef\u06e7\3\2\2\2\u06ef\u06f0\3\2\2\2\u06f0\u06f3\3\2"+
		"\2\2\u06f1\u06f3\5\u009eP\2\u06f2\u06db\3\2\2\2\u06f2\u06df\3\2\2\2\u06f2"+
		"\u06e2\3\2\2\2\u06f2\u06f1\3\2\2\2\u06f3w\3\2\2\2\u06f4\u06f5\7\3\2\2"+
		"\u06f5\u06f6\5z>\2\u06f6\u06f7\7\4\2\2\u06f7y\3\2\2\2\u06f8\u06fd\5\u00d4"+
		"k\2\u06f9\u06fa\7\5\2\2\u06fa\u06fc\5\u00d4k\2\u06fb\u06f9\3\2\2\2\u06fc"+
		"\u06ff\3\2\2\2\u06fd\u06fb\3\2\2\2\u06fd\u06fe\3\2\2\2\u06fe{\3\2\2\2"+
		"\u06ff\u06fd\3\2\2\2\u0700\u0701\7\3\2\2\u0701\u0706\5~@\2\u0702\u0703"+
		"\7\5\2\2\u0703\u0705\5~@\2\u0704\u0702\3\2\2\2\u0705\u0708\3\2\2\2\u0706"+
		"\u0704\3\2\2\2\u0706\u0707\3\2\2\2\u0707\u0709\3\2\2\2\u0708\u0706\3\2"+
		"\2\2\u0709\u070a\7\4\2\2\u070a}\3\2\2\2\u070b\u070d\5\u00d4k\2\u070c\u070e"+
		"\t\f\2\2\u070d\u070c\3\2\2\2\u070d\u070e\3\2\2\2\u070e\177\3\2\2\2\u070f"+
		"\u0710\7\3\2\2\u0710\u0715\5\u0082B\2\u0711\u0712\7\5\2\2\u0712\u0714"+
		"\5\u0082B\2\u0713\u0711\3\2\2\2\u0714\u0717\3\2\2\2\u0715\u0713\3\2\2"+
		"\2\u0715\u0716\3\2\2\2\u0716\u0718\3\2\2\2\u0717\u0715\3\2\2\2\u0718\u0719"+
		"\7\4\2\2\u0719\u0081\3\2\2\2\u071a\u071d\5\u00d4k\2\u071b\u071c\7.\2\2"+
		"\u071c\u071e\7\u0118\2\2\u071d\u071b\3\2\2\2\u071d\u071e\3\2\2\2\u071e"+
		"\u0083\3\2\2\2\u071f\u0721\5\u0090I\2\u0720\u0722\5t;\2\u0721\u0720\3"+
		"\2\2\2\u0721\u0722\3\2\2\2\u0722\u0723\3\2\2\2\u0723\u0724\5\u008aF\2"+
		"\u0724\u0738\3\2\2\2\u0725\u0726\7\3\2\2\u0726\u0727\5H%\2\u0727\u0729"+
		"\7\4\2\2\u0728\u072a\5t;\2\u0729\u0728\3\2\2\2\u0729\u072a\3\2\2\2\u072a"+
		"\u072b\3\2\2\2\u072b\u072c\5\u008aF\2\u072c\u0738\3\2\2\2\u072d\u072e"+
		"\7\3\2\2\u072e\u072f\5l\67\2\u072f\u0731\7\4\2\2\u0730\u0732\5t;\2\u0731"+
		"\u0730\3\2\2\2\u0731\u0732\3\2\2\2\u0732\u0733\3\2\2\2\u0733\u0734\5\u008a"+
		"F\2\u0734\u0738\3\2\2\2\u0735\u0738\5\u0086D\2\u0736\u0738\5\u0088E\2"+
		"\u0737\u071f\3\2\2\2\u0737\u0725\3\2\2\2\u0737\u072d\3\2\2\2\u0737\u0735"+
		"\3\2\2\2\u0737\u0736\3\2\2\2\u0738\u0085\3\2\2\2\u0739\u073a\7\u00fb\2"+
		"\2\u073a\u073f\5\u009eP\2\u073b\u073c\7\5\2\2\u073c\u073e\5\u009eP\2\u073d"+
		"\u073b\3\2\2\2\u073e\u0741\3\2\2\2\u073f\u073d\3\2\2\2\u073f\u0740\3\2"+
		"\2\2\u0740\u0742\3\2\2\2\u0741\u073f\3\2\2\2\u0742\u0743\5\u008aF\2\u0743"+
		"\u0087\3\2\2\2\u0744\u0745\5\u00d4k\2\u0745\u074e\7\3\2\2\u0746\u074b"+
		"\5\u009eP\2\u0747\u0748\7\5\2\2\u0748\u074a\5\u009eP\2\u0749\u0747\3\2"+
		"\2\2\u074a\u074d\3\2\2\2\u074b\u0749\3\2\2\2\u074b\u074c\3\2\2\2\u074c"+
		"\u074f\3\2\2\2\u074d\u074b\3\2\2\2\u074e\u0746\3\2\2\2\u074e\u074f\3\2"+
		"\2\2\u074f\u0750\3\2\2\2\u0750\u0751\7\4\2\2\u0751\u0752\5\u008aF\2\u0752"+
		"\u0089\3\2\2\2\u0753\u0755\7\27\2\2\u0754\u0753\3\2\2\2\u0754\u0755\3"+
		"\2\2\2\u0755\u0756\3\2\2\2\u0756\u0758\5\u00d6l\2\u0757\u0759\5x=\2\u0758"+
		"\u0757\3\2\2\2\u0758\u0759\3\2\2\2\u0759\u075b\3\2\2\2\u075a\u0754\3\2"+
		"\2\2\u075a\u075b\3\2\2\2\u075b\u008b\3\2\2\2\u075c\u075d\7\u00ca\2\2\u075d"+
		"\u075e\7b\2\2\u075e\u075f\7\u00d2\2\2\u075f\u0763\7\u0118\2\2\u0760\u0761"+
		"\7\u0102\2\2\u0761\u0762\7\u00d3\2\2\u0762\u0764\5\62\32\2\u0763\u0760"+
		"\3\2\2\2\u0763\u0764\3\2\2\2\u0764\u078e\3\2\2\2\u0765\u0766\7\u00ca\2"+
		"\2\u0766\u0767\7b\2\2\u0767\u0771\7F\2\2\u0768\u0769\7\\\2\2\u0769\u076a"+
		"\7\u00e7\2\2\u076a\u076b\7\37\2\2\u076b\u076f\7\u0118\2\2\u076c\u076d"+
		"\7Q\2\2\u076d\u076e\7\37\2\2\u076e\u0770\7\u0118\2\2\u076f\u076c\3\2\2"+
		"\2\u076f\u0770\3\2\2\2\u0770\u0772\3\2\2\2\u0771\u0768\3\2\2\2\u0771\u0772"+
		"\3\2\2\2\u0772\u0778\3\2\2\2\u0773\u0774\7+\2\2\u0774\u0775\7}\2\2\u0775"+
		"\u0776\7\u00e7\2\2\u0776\u0777\7\37\2\2\u0777\u0779\7\u0118\2\2\u0778"+
		"\u0773\3\2\2\2\u0778\u0779\3\2\2\2\u0779\u077f\3\2\2\2\u077a\u077b\7\u0090"+
		"\2\2\u077b\u077c\7\177\2\2\u077c\u077d\7\u00e7\2\2\u077d\u077e\7\37\2"+
		"\2\u077e\u0780\7\u0118\2\2\u077f\u077a\3\2\2\2\u077f\u0780\3\2\2\2\u0780"+
		"\u0785\3\2\2\2\u0781\u0782\7\u0087\2\2\u0782\u0783\7\u00e7\2\2\u0783\u0784"+
		"\7\37\2\2\u0784\u0786\7\u0118\2\2\u0785\u0781\3\2\2\2\u0785\u0786\3\2"+
		"\2\2\u0786\u078b\3\2\2\2\u0787\u0788\7\u009d\2\2\u0788\u0789\7D\2\2\u0789"+
		"\u078a\7\27\2\2\u078a\u078c\7\u0118\2\2\u078b\u0787\3\2\2\2\u078b\u078c"+
		"\3\2\2\2\u078c\u078e\3\2\2\2\u078d\u075c\3\2\2\2\u078d\u0765\3\2\2\2\u078e"+
		"\u008d\3\2\2\2\u078f\u0794\5\u00d4k\2\u0790\u0791\7\6\2\2\u0791\u0793"+
		"\5\u00d4k\2\u0792\u0790\3\2\2\2\u0793\u0796\3\2\2\2\u0794\u0792\3\2\2"+
		"\2\u0794\u0795\3\2\2\2\u0795\u008f\3\2\2\2\u0796\u0794\3\2\2\2\u0797\u0798"+
		"\5\u00d4k\2\u0798\u0799\7\6\2\2\u0799\u079b\3\2\2\2\u079a\u0797\3\2\2"+
		"\2\u079a\u079b\3\2\2\2\u079b\u079c\3\2\2\2\u079c\u079d\5\u00d4k\2\u079d"+
		"\u0091\3\2\2\2\u079e\u079f\5\u00d4k\2\u079f\u07a0\7\6\2\2\u07a0\u07a2"+
		"\3\2\2\2\u07a1\u079e\3\2\2\2\u07a1\u07a2\3\2\2\2\u07a2\u07a3\3\2\2\2\u07a3"+
		"\u07a4\5\u00d4k\2\u07a4\u0093\3\2\2\2\u07a5\u07ad\5\u009eP\2\u07a6\u07a8"+
		"\7\27\2\2\u07a7\u07a6\3\2\2\2\u07a7\u07a8\3\2\2\2\u07a8\u07ab\3\2\2\2"+
		"\u07a9\u07ac\5\u00d4k\2\u07aa\u07ac\5x=\2\u07ab\u07a9\3\2\2\2\u07ab\u07aa"+
		"\3\2\2\2\u07ac\u07ae\3\2\2\2\u07ad\u07a7\3\2\2\2\u07ad\u07ae\3\2\2\2\u07ae"+
		"\u0095\3\2\2\2\u07af\u07b4\5\u0094K\2\u07b0\u07b1\7\5\2\2\u07b1\u07b3"+
		"\5\u0094K\2\u07b2\u07b0\3\2\2\2\u07b3\u07b6\3\2\2\2\u07b4\u07b2\3\2\2"+
		"\2\u07b4\u07b5\3\2\2\2\u07b5\u0097\3\2\2\2\u07b6\u07b4\3\2\2\2\u07b7\u07b8"+
		"\7\3\2\2\u07b8\u07bd\5\u009aN\2\u07b9\u07ba\7\5\2\2\u07ba\u07bc\5\u009a"+
		"N\2\u07bb\u07b9\3\2\2\2\u07bc\u07bf\3\2\2\2\u07bd\u07bb\3\2\2\2\u07bd"+
		"\u07be\3\2\2\2\u07be\u07c0\3\2\2\2\u07bf\u07bd\3\2\2\2\u07c0\u07c1\7\4"+
		"\2\2\u07c1\u0099\3\2\2\2\u07c2\u07d0\5\u00d2j\2\u07c3\u07c4\5\u00d4k\2"+
		"\u07c4\u07c5\7\3\2\2\u07c5\u07ca\5\u009cO\2\u07c6\u07c7\7\5\2\2\u07c7"+
		"\u07c9\5\u009cO\2\u07c8\u07c6\3\2\2\2\u07c9\u07cc\3\2\2\2\u07ca\u07c8"+
		"\3\2\2\2\u07ca\u07cb\3\2\2\2\u07cb\u07cd\3\2\2\2\u07cc\u07ca\3\2\2\2\u07cd"+
		"\u07ce\7\4\2\2\u07ce\u07d0\3\2\2\2\u07cf\u07c2\3\2\2\2\u07cf\u07c3\3\2"+
		"\2\2\u07d0\u009b\3\2\2\2\u07d1\u07d4\5\u00d2j\2\u07d2\u07d4\5\u00a8U\2"+
		"\u07d3\u07d1\3\2\2\2\u07d3\u07d2\3\2\2\2\u07d4\u009d\3\2\2\2\u07d5\u07d6"+
		"\5\u00a0Q\2\u07d6\u009f\3\2\2\2\u07d7\u07d8\bQ\1\2\u07d8\u07d9\7\u009c"+
		"\2\2\u07d9\u07e4\5\u00a0Q\7\u07da\u07db\7T\2\2\u07db\u07dc\7\3\2\2\u07dc"+
		"\u07dd\5\34\17\2\u07dd\u07de\7\4\2\2\u07de\u07e4\3\2\2\2\u07df\u07e1\5"+
		"\u00a4S\2\u07e0\u07e2\5\u00a2R\2\u07e1\u07e0\3\2\2\2\u07e1\u07e2\3\2\2"+
		"\2\u07e2\u07e4\3\2\2\2\u07e3\u07d7\3\2\2\2\u07e3\u07da\3\2\2\2\u07e3\u07df"+
		"\3\2\2\2\u07e4\u07ed\3\2\2\2\u07e5\u07e6\f\4\2\2\u07e6\u07e7\7\22\2\2"+
		"\u07e7\u07ec\5\u00a0Q\5\u07e8\u07e9\f\3\2\2\u07e9\u07ea\7\u00a4\2\2\u07ea"+
		"\u07ec\5\u00a0Q\4\u07eb\u07e5\3\2\2\2\u07eb\u07e8\3\2\2\2\u07ec\u07ef"+
		"\3\2\2\2\u07ed\u07eb\3\2\2\2\u07ed\u07ee\3\2\2\2\u07ee\u00a1\3\2\2\2\u07ef"+
		"\u07ed\3\2\2\2\u07f0\u07f2\7\u009c\2\2\u07f1\u07f0\3\2\2\2\u07f1\u07f2"+
		"\3\2\2\2\u07f2\u07f3\3\2\2\2\u07f3\u07f4\7\33\2\2\u07f4\u07f5\5\u00a4"+
		"S\2\u07f5\u07f6\7\22\2\2\u07f6\u07f7\5\u00a4S\2\u07f7\u0821\3\2\2\2\u07f8"+
		"\u07fa\7\u009c\2\2\u07f9\u07f8\3\2\2\2\u07f9\u07fa\3\2\2\2\u07fa\u07fb"+
		"\3\2\2\2\u07fb\u07fc\7r\2\2\u07fc\u07fd\7\3\2\2\u07fd\u0802\5\u009eP\2"+
		"\u07fe\u07ff\7\5\2\2\u07ff\u0801\5\u009eP\2\u0800\u07fe\3\2\2\2\u0801"+
		"\u0804\3\2\2\2\u0802\u0800\3\2\2\2\u0802\u0803\3\2\2\2\u0803\u0805\3\2"+
		"\2\2\u0804\u0802\3\2\2\2\u0805\u0806\7\4\2\2\u0806\u0821\3\2\2\2\u0807"+
		"\u0809\7\u009c\2\2\u0808\u0807\3\2\2\2\u0808\u0809\3\2\2\2\u0809\u080a"+
		"\3\2\2\2\u080a\u080b\7r\2\2\u080b\u080c\7\3\2\2\u080c\u080d\5\34\17\2"+
		"\u080d\u080e\7\4\2\2\u080e\u0821\3\2\2\2\u080f\u0811\7\u009c\2\2\u0810"+
		"\u080f\3\2\2\2\u0810\u0811\3\2\2\2\u0811\u0812\3\2\2\2\u0812\u0813\t\20"+
		"\2\2\u0813\u0821\5\u00a4S\2\u0814\u0816\7|\2\2\u0815\u0817\7\u009c\2\2"+
		"\u0816\u0815\3\2\2\2\u0816\u0817\3\2\2\2\u0817\u0818\3\2\2\2\u0818\u0821"+
		"\7\u009d\2\2\u0819\u081b\7|\2\2\u081a\u081c\7\u009c\2\2\u081b\u081a\3"+
		"\2\2\2\u081b\u081c\3\2\2\2\u081c\u081d\3\2\2\2\u081d\u081e\7L\2\2\u081e"+
		"\u081f\7d\2\2\u081f\u0821\5\u00a4S\2\u0820\u07f1\3\2\2\2\u0820\u07f9\3"+
		"\2\2\2\u0820\u0808\3\2\2\2\u0820\u0810\3\2\2\2\u0820\u0814\3\2\2\2\u0820"+
		"\u0819\3\2\2\2\u0821\u00a3\3\2\2\2\u0822\u0823\bS\1\2\u0823\u0827\5\u00a6"+
		"T\2\u0824\u0825\t\21\2\2\u0825\u0827\5\u00a4S\t\u0826\u0822\3\2\2\2\u0826"+
		"\u0824\3\2\2\2\u0827\u083d\3\2\2\2\u0828\u0829\f\b\2\2\u0829\u082a\t\22"+
		"\2\2\u082a\u083c\5\u00a4S\t\u082b\u082c\f\7\2\2\u082c\u082d\t\23\2\2\u082d"+
		"\u083c\5\u00a4S\b\u082e\u082f\f\6\2\2\u082f\u0830\7\u0114\2\2\u0830\u083c"+
		"\5\u00a4S\7\u0831\u0832\f\5\2\2\u0832\u0833\7\u0117\2\2\u0833\u083c\5"+
		"\u00a4S\6\u0834\u0835\f\4\2\2\u0835\u0836\7\u0115\2\2\u0836\u083c\5\u00a4"+
		"S\5\u0837\u0838\f\3\2\2\u0838\u0839\5\u00aaV\2\u0839\u083a\5\u00a4S\4"+
		"\u083a\u083c\3\2\2\2\u083b\u0828\3\2\2\2\u083b\u082b\3\2\2\2\u083b\u082e"+
		"\3\2\2\2\u083b\u0831\3\2\2\2\u083b\u0834\3\2\2\2\u083b\u0837\3\2\2\2\u083c"+
		"\u083f\3\2\2\2\u083d\u083b\3\2\2\2\u083d\u083e\3\2\2\2\u083e\u00a5\3\2"+
		"\2\2\u083f\u083d\3\2\2\2\u0840\u0841\bT\1\2\u0841\u08d3\t\24\2\2\u0842"+
		"\u0844\7\"\2\2\u0843\u0845\5\u00c6d\2\u0844\u0843\3\2\2\2\u0845\u0846"+
		"\3\2\2\2\u0846\u0844\3\2\2\2\u0846\u0847\3\2\2\2\u0847\u084a\3\2\2\2\u0848"+
		"\u0849\7O\2\2\u0849\u084b\5\u009eP\2\u084a\u0848\3\2\2\2\u084a\u084b\3"+
		"\2\2\2\u084b\u084c\3\2\2\2\u084c\u084d\7P\2\2\u084d\u08d3\3\2\2\2\u084e"+
		"\u084f\7\"\2\2\u084f\u0851\5\u009eP\2\u0850\u0852\5\u00c6d\2\u0851\u0850"+
		"\3\2\2\2\u0852\u0853\3\2\2\2\u0853\u0851\3\2\2\2\u0853\u0854\3\2\2\2\u0854"+
		"\u0857\3\2\2\2\u0855\u0856\7O\2\2\u0856\u0858\5\u009eP\2\u0857\u0855\3"+
		"\2\2\2\u0857\u0858\3\2\2\2\u0858\u0859\3\2\2\2\u0859\u085a\7P\2\2\u085a"+
		"\u08d3\3\2\2\2\u085b\u085c\7#\2\2\u085c\u085d\7\3\2\2\u085d\u085e\5\u009e"+
		"P\2\u085e\u085f\7\27\2\2\u085f\u0860\5\u00bc_\2\u0860\u0861\7\4\2\2\u0861"+
		"\u08d3\3\2\2\2\u0862\u0863\7\u00e1\2\2\u0863\u086c\7\3\2\2\u0864\u0869"+
		"\5\u0094K\2\u0865\u0866\7\5\2\2\u0866\u0868\5\u0094K\2\u0867\u0865\3\2"+
		"\2\2\u0868\u086b\3\2\2\2\u0869\u0867\3\2\2\2\u0869\u086a\3\2\2\2\u086a"+
		"\u086d\3\2\2\2\u086b\u0869\3\2\2\2\u086c\u0864\3\2\2\2\u086c\u086d\3\2"+
		"\2\2\u086d\u086e\3\2\2\2\u086e\u08d3\7\4\2\2\u086f\u0870\7^\2\2\u0870"+
		"\u0871\7\3\2\2\u0871\u0874\5\u009eP\2\u0872\u0873\7p\2\2\u0873\u0875\7"+
		"\u009e\2\2\u0874\u0872\3\2\2\2\u0874\u0875\3\2\2\2\u0875\u0876\3\2\2\2"+
		"\u0876\u0877\7\4\2\2\u0877\u08d3\3\2\2\2\u0878\u0879\7\u0080\2\2\u0879"+
		"\u087a\7\3\2\2\u087a\u087d\5\u009eP\2\u087b\u087c\7p\2\2\u087c\u087e\7"+
		"\u009e\2\2\u087d\u087b\3\2\2\2\u087d\u087e\3\2\2\2\u087e\u087f\3\2\2\2"+
		"\u087f\u0880\7\4\2\2\u0880\u08d3\3\2\2\2\u0881\u0882\7\u00b1\2\2\u0882"+
		"\u0883\7\3\2\2\u0883\u0884\5\u00a4S\2\u0884\u0885\7r\2\2\u0885\u0886\5"+
		"\u00a4S\2\u0886\u0887\7\4\2\2\u0887\u08d3\3\2\2\2\u0888\u08d3\5\u00a8"+
		"U\2\u0889\u08d3\7\u010f\2\2\u088a\u088b\5\u00d2j\2\u088b\u088c\7\6\2\2"+
		"\u088c\u088d\7\u010f\2\2\u088d\u08d3\3\2\2\2\u088e\u088f\7\3\2\2\u088f"+
		"\u0892\5\u0094K\2\u0890\u0891\7\5\2\2\u0891\u0893\5\u0094K\2\u0892\u0890"+
		"\3\2\2\2\u0893\u0894\3\2\2\2\u0894\u0892\3\2\2\2\u0894\u0895\3\2\2\2\u0895"+
		"\u0896\3\2\2\2\u0896\u0897\7\4\2\2\u0897\u08d3\3\2\2\2\u0898\u0899\7\3"+
		"\2\2\u0899\u089a\5\34\17\2\u089a\u089b\7\4\2\2\u089b\u08d3\3\2\2\2\u089c"+
		"\u089d\5\u00d2j\2\u089d\u08a9\7\3\2\2\u089e\u08a0\5j\66\2\u089f\u089e"+
		"\3\2\2\2\u089f\u08a0\3\2\2\2\u08a0\u08a1\3\2\2\2\u08a1\u08a6\5\u009eP"+
		"\2\u08a2\u08a3\7\5\2\2\u08a3\u08a5\5\u009eP\2\u08a4\u08a2\3\2\2\2\u08a5"+
		"\u08a8\3\2\2\2\u08a6\u08a4\3\2\2\2\u08a6\u08a7\3\2\2\2\u08a7\u08aa\3\2"+
		"\2\2\u08a8\u08a6\3\2\2\2\u08a9\u089f\3\2\2\2\u08a9\u08aa\3\2\2\2\u08aa"+
		"\u08ab\3\2\2\2\u08ab\u08ae\7\4\2\2\u08ac\u08ad\7\u00a9\2\2\u08ad\u08af"+
		"\5\u00ccg\2\u08ae\u08ac\3\2\2\2\u08ae\u08af\3\2\2\2\u08af\u08d3\3\2\2"+
		"\2\u08b0\u08b1\5\u00d2j\2\u08b1\u08b2\7\3\2\2\u08b2\u08b3\t\25\2\2\u08b3"+
		"\u08b4\5\u009eP\2\u08b4\u08b5\7d\2\2\u08b5\u08b6\5\u009eP\2\u08b6\u08b7"+
		"\7\4\2\2\u08b7\u08d3\3\2\2\2\u08b8\u08b9\7\u0120\2\2\u08b9\u08ba\7\t\2"+
		"\2\u08ba\u08d3\5\u009eP\2\u08bb\u08bc\7\3\2\2\u08bc\u08bf\7\u0120\2\2"+
		"\u08bd\u08be\7\5\2\2\u08be\u08c0\7\u0120\2\2\u08bf\u08bd\3\2\2\2\u08c0"+
		"\u08c1\3\2\2\2\u08c1\u08bf\3\2\2\2\u08c1\u08c2\3\2\2\2\u08c2\u08c3\3\2"+
		"\2\2\u08c3\u08c4\7\4\2\2\u08c4\u08c5\7\t\2\2\u08c5\u08d3\5\u009eP\2\u08c6"+
		"\u08d3\5\u00d4k\2\u08c7\u08c8\7\3\2\2\u08c8\u08c9\5\u009eP\2\u08c9\u08ca"+
		"\7\4\2\2\u08ca\u08d3\3\2\2\2\u08cb\u08cc\7Y\2\2\u08cc\u08cd\7\3\2\2\u08cd"+
		"\u08ce\5\u00d4k\2\u08ce\u08cf\7d\2\2\u08cf\u08d0\5\u00a4S\2\u08d0\u08d1"+
		"\7\4\2\2\u08d1\u08d3\3\2\2\2\u08d2\u0840\3\2\2\2\u08d2\u0842\3\2\2\2\u08d2"+
		"\u084e\3\2\2\2\u08d2\u085b\3\2\2\2\u08d2\u0862\3\2\2\2\u08d2\u086f\3\2"+
		"\2\2\u08d2\u0878\3\2\2\2\u08d2\u0881\3\2\2\2\u08d2\u0888\3\2\2\2\u08d2"+
		"\u0889\3\2\2\2\u08d2\u088a\3\2\2\2\u08d2\u088e\3\2\2\2\u08d2\u0898\3\2"+
		"\2\2\u08d2\u089c\3\2\2\2\u08d2\u08b0\3\2\2\2\u08d2\u08b8\3\2\2\2\u08d2"+
		"\u08bb\3\2\2\2\u08d2\u08c6\3\2\2\2\u08d2\u08c7\3\2\2\2\u08d2\u08cb\3\2"+
		"\2\2\u08d3\u08de\3\2\2\2\u08d4\u08d5\f\7\2\2\u08d5\u08d6\7\n\2\2\u08d6"+
		"\u08d7\5\u00a4S\2\u08d7\u08d8\7\13\2\2\u08d8\u08dd\3\2\2\2\u08d9\u08da"+
		"\f\5\2\2\u08da\u08db\7\6\2\2\u08db\u08dd\5\u00d4k\2\u08dc\u08d4\3\2\2"+
		"\2\u08dc\u08d9\3\2\2\2\u08dd\u08e0\3\2\2\2\u08de\u08dc\3\2\2\2\u08de\u08df"+
		"\3\2\2\2\u08df\u00a7\3\2\2\2\u08e0\u08de\3\2\2\2\u08e1\u08ee\7\u009d\2"+
		"\2\u08e2\u08ee\5\u00b2Z\2\u08e3\u08e4\5\u00d4k\2\u08e4\u08e5\7\u0118\2"+
		"\2\u08e5\u08ee\3\2\2\2\u08e6\u08ee\5\u00dan\2\u08e7\u08ee\5\u00b0Y\2\u08e8"+
		"\u08ea\7\u0118\2\2\u08e9\u08e8\3\2\2\2\u08ea\u08eb\3\2\2\2\u08eb\u08e9"+
		"\3\2\2\2\u08eb\u08ec\3\2\2\2\u08ec\u08ee\3\2\2\2\u08ed\u08e1\3\2\2\2\u08ed"+
		"\u08e2\3\2\2\2\u08ed\u08e3\3\2\2\2\u08ed\u08e6\3\2\2\2\u08ed\u08e7\3\2"+
		"\2\2\u08ed\u08e9\3\2\2\2\u08ee\u00a9\3\2\2\2\u08ef\u08f0\t\26\2\2\u08f0"+
		"\u00ab\3\2\2\2\u08f1\u08f2\t\27\2\2";
	private static final String _serializedATNSegment1 =
		"\u08f2\u00ad\3\2\2\2\u08f3\u08f4\t\30\2\2\u08f4\u00af\3\2\2\2\u08f5\u08f6"+
		"\t\31\2\2\u08f6\u00b1\3\2\2\2\u08f7\u08f9\6Z\22\2\u08f8\u08fa\7z\2\2\u08f9"+
		"\u08f8\3\2\2\2\u08f9\u08fa\3\2\2\2\u08fa\u08fc\3\2\2\2\u08fb\u08fd\5\u00b4"+
		"[\2\u08fc\u08fb\3\2\2\2\u08fd\u08fe\3\2\2\2\u08fe\u08fc\3\2\2\2\u08fe"+
		"\u08ff\3\2\2\2\u08ff\u0909\3\2\2\2\u0900\u0901\6Z\23\2\u0901\u0905\7z"+
		"\2\2\u0902\u0904\5\u00b4[\2\u0903\u0902\3\2\2\2\u0904\u0907\3\2\2\2\u0905"+
		"\u0903\3\2\2\2\u0905\u0906\3\2\2\2\u0906\u0909\3\2\2\2\u0907\u0905\3\2"+
		"\2\2\u0908\u08f7\3\2\2\2\u0908\u0900\3\2\2\2\u0909\u00b3\3\2\2\2\u090a"+
		"\u090b\5\u00b6\\\2\u090b\u090e\5\u00b8]\2\u090c\u090d\7\u00e9\2\2\u090d"+
		"\u090f\5\u00b8]\2\u090e\u090c\3\2\2\2\u090e\u090f\3\2\2\2\u090f\u00b5"+
		"\3\2\2\2\u0910\u0912\t\32\2\2\u0911\u0910\3\2\2\2\u0911\u0912\3\2\2\2"+
		"\u0912\u0913\3\2\2\2\u0913\u0916\t\17\2\2\u0914\u0916\7\u0118\2\2\u0915"+
		"\u0911\3\2\2\2\u0915\u0914\3\2\2\2\u0916\u00b7\3\2\2\2\u0917\u0918\t\33"+
		"\2\2\u0918\u00b9\3\2\2\2\u0919\u091d\7^\2\2\u091a\u091b\7\16\2\2\u091b"+
		"\u091d\5\u00d4k\2\u091c\u0919\3\2\2\2\u091c\u091a\3\2\2\2\u091d\u00bb"+
		"\3\2\2\2\u091e\u091f\7\26\2\2\u091f\u0920\7\u0109\2\2\u0920\u0921\5\u00bc"+
		"_\2\u0921\u0922\7\u010b\2\2\u0922\u0941\3\2\2\2\u0923\u0924\7\u0090\2"+
		"\2\u0924\u0925\7\u0109\2\2\u0925\u0926\5\u00bc_\2\u0926\u0927\7\5\2\2"+
		"\u0927\u0928\5\u00bc_\2\u0928\u0929\7\u010b\2\2\u0929\u0941\3\2\2\2\u092a"+
		"\u0931\7\u00e1\2\2\u092b\u092d\7\u0109\2\2\u092c\u092e\5\u00c2b\2\u092d"+
		"\u092c\3\2\2\2\u092d\u092e\3\2\2\2\u092e\u092f\3\2\2\2\u092f\u0932\7\u010b"+
		"\2\2\u0930\u0932\7\u0107\2\2\u0931\u092b\3\2\2\2\u0931\u0930\3\2\2\2\u0932"+
		"\u0941\3\2\2\2\u0933\u093e\5\u00d4k\2\u0934\u0935\7\3\2\2\u0935\u093a"+
		"\7\u011c\2\2\u0936\u0937\7\5\2\2\u0937\u0939\7\u011c\2\2\u0938\u0936\3"+
		"\2\2\2\u0939\u093c\3\2\2\2\u093a\u0938\3\2\2\2\u093a\u093b\3\2\2\2\u093b"+
		"\u093d\3\2\2\2\u093c\u093a\3\2\2\2\u093d\u093f\7\4\2\2\u093e\u0934\3\2"+
		"\2\2\u093e\u093f\3\2\2\2\u093f\u0941\3\2\2\2\u0940\u091e\3\2\2\2\u0940"+
		"\u0923\3\2\2\2\u0940\u092a\3\2\2\2\u0940\u0933\3\2\2\2\u0941\u00bd\3\2"+
		"\2\2\u0942\u0947\5\u00c0a\2\u0943\u0944\7\5\2\2\u0944\u0946\5\u00c0a\2"+
		"\u0945\u0943\3\2\2\2\u0946\u0949\3\2\2\2\u0947\u0945\3\2\2\2\u0947\u0948"+
		"\3\2\2\2\u0948\u00bf\3\2\2\2\u0949\u0947\3\2\2\2\u094a\u094b\5\u00d4k"+
		"\2\u094b\u094e\5\u00bc_\2\u094c\u094d\7.\2\2\u094d\u094f\7\u0118\2\2\u094e"+
		"\u094c\3\2\2\2\u094e\u094f\3\2\2\2\u094f\u00c1\3\2\2\2\u0950\u0955\5\u00c4"+
		"c\2\u0951\u0952\7\5\2\2\u0952\u0954\5\u00c4c\2\u0953\u0951\3\2\2\2\u0954"+
		"\u0957\3\2\2\2\u0955\u0953\3\2\2\2\u0955\u0956\3\2\2\2\u0956\u00c3\3\2"+
		"\2\2\u0957\u0955\3\2\2\2\u0958\u0959\5\u00d4k\2\u0959\u095a\7\f\2\2\u095a"+
		"\u095d\5\u00bc_\2\u095b\u095c\7.\2\2\u095c\u095e\7\u0118\2\2\u095d\u095b"+
		"\3\2\2\2\u095d\u095e\3\2\2\2\u095e\u00c5\3\2\2\2\u095f\u0960\7\u00ff\2"+
		"\2\u0960\u0961\5\u009eP\2\u0961\u0962\7\u00e8\2\2\u0962\u0963\5\u009e"+
		"P\2\u0963\u00c7\3\2\2\2\u0964\u0965\7\u0101\2\2\u0965\u096a\5\u00caf\2"+
		"\u0966\u0967\7\5\2\2\u0967\u0969\5\u00caf\2\u0968\u0966\3\2\2\2\u0969"+
		"\u096c\3\2\2\2\u096a\u0968\3\2\2\2\u096a\u096b\3\2\2\2\u096b\u00c9\3\2"+
		"\2\2\u096c\u096a\3\2\2\2\u096d\u096e\5\u00d4k\2\u096e\u096f\7\27\2\2\u096f"+
		"\u0970\5\u00ccg\2\u0970\u00cb\3\2\2\2\u0971\u09a0\5\u00d4k\2\u0972\u0973"+
		"\7\3\2\2\u0973\u0974\5\u00d4k\2\u0974\u0975\7\4\2\2\u0975\u09a0\3\2\2"+
		"\2\u0976\u0999\7\3\2\2\u0977\u0978\7\'\2\2\u0978\u0979\7\37\2\2\u0979"+
		"\u097e\5\u009eP\2\u097a\u097b\7\5\2\2\u097b\u097d\5\u009eP\2\u097c\u097a"+
		"\3\2\2\2\u097d\u0980\3\2\2\2\u097e\u097c\3\2\2\2\u097e\u097f\3\2\2\2\u097f"+
		"\u099a\3\2\2\2\u0980\u097e\3\2\2\2\u0981\u0982\t\34\2\2\u0982\u0983\7"+
		"\37\2\2\u0983\u0988\5\u009eP\2\u0984\u0985\7\5\2\2\u0985\u0987\5\u009e"+
		"P\2\u0986\u0984\3\2\2\2\u0987\u098a\3\2\2\2\u0988\u0986\3\2\2\2\u0988"+
		"\u0989\3\2\2\2\u0989\u098c\3\2\2\2\u098a\u0988\3\2\2\2\u098b\u0981\3\2"+
		"\2\2\u098b\u098c\3\2\2\2\u098c\u0997\3\2\2\2\u098d\u098e\t\35\2\2\u098e"+
		"\u098f\7\37\2\2\u098f\u0994\5T+\2\u0990\u0991\7\5\2\2\u0991\u0993\5T+"+
		"\2\u0992\u0990\3\2\2\2\u0993\u0996\3\2\2\2\u0994\u0992\3\2\2\2\u0994\u0995"+
		"\3\2\2\2\u0995\u0998\3\2\2\2\u0996\u0994\3\2\2\2\u0997\u098d\3\2\2\2\u0997"+
		"\u0998\3\2\2\2\u0998\u099a\3\2\2\2\u0999\u0977\3\2\2\2\u0999\u098b\3\2"+
		"\2\2\u099a\u099c\3\2\2\2\u099b\u099d\5\u00ceh\2\u099c\u099b\3\2\2\2\u099c"+
		"\u099d\3\2\2\2\u099d\u099e\3\2\2\2\u099e\u09a0\7\4\2\2\u099f\u0971\3\2"+
		"\2\2\u099f\u0972\3\2\2\2\u099f\u0976\3\2\2\2\u09a0\u00cd\3\2\2\2\u09a1"+
		"\u09a2\7\u00b7\2\2\u09a2\u09b2\5\u00d0i\2\u09a3\u09a4\7\u00cb\2\2\u09a4"+
		"\u09b2\5\u00d0i\2\u09a5\u09a6\7\u00b7\2\2\u09a6\u09a7\7\33\2\2\u09a7\u09a8"+
		"\5\u00d0i\2\u09a8\u09a9\7\22\2\2\u09a9\u09aa\5\u00d0i\2\u09aa\u09b2\3"+
		"\2\2\2\u09ab\u09ac\7\u00cb\2\2\u09ac\u09ad\7\33\2\2\u09ad\u09ae\5\u00d0"+
		"i\2\u09ae\u09af\7\22\2\2\u09af\u09b0\5\u00d0i\2\u09b0\u09b2\3\2\2\2\u09b1"+
		"\u09a1\3\2\2\2\u09b1\u09a3\3\2\2\2\u09b1\u09a5\3\2\2\2\u09b1\u09ab\3\2"+
		"\2\2\u09b2\u00cf\3\2\2\2\u09b3\u09b4\7\u00f2\2\2\u09b4\u09bb\t\36\2\2"+
		"\u09b5\u09b6\79\2\2\u09b6\u09bb\7\u00ca\2\2\u09b7\u09b8\5\u009eP\2\u09b8"+
		"\u09b9\t\36\2\2\u09b9\u09bb\3\2\2\2\u09ba\u09b3\3\2\2\2\u09ba\u09b5\3"+
		"\2\2\2\u09ba\u09b7\3\2\2\2\u09bb\u00d1\3\2\2\2\u09bc\u09c1\5\u00d4k\2"+
		"\u09bd\u09be\7\6\2\2\u09be\u09c0\5\u00d4k\2\u09bf\u09bd\3\2\2\2\u09c0"+
		"\u09c3\3\2\2\2\u09c1\u09bf\3\2\2\2\u09c1\u09c2\3\2\2\2\u09c2\u00d3\3\2"+
		"\2\2\u09c3\u09c1\3\2\2\2\u09c4\u09c8\5\u00d6l\2\u09c5\u09c6\6k\24\2\u09c6"+
		"\u09c8\5\u00dep\2\u09c7\u09c4\3\2\2\2\u09c7\u09c5\3\2\2\2\u09c8\u00d5"+
		"\3\2\2\2\u09c9\u09d0\7\u0120\2\2\u09ca\u09d0\5\u00d8m\2\u09cb\u09cc\6"+
		"l\25\2\u09cc\u09d0\5\u00dco\2\u09cd\u09ce\6l\26\2\u09ce\u09d0\5\u00e0"+
		"q\2\u09cf\u09c9\3\2\2\2\u09cf\u09ca\3\2\2\2\u09cf\u09cb\3\2\2\2\u09cf"+
		"\u09cd\3\2\2\2\u09d0\u00d7\3\2\2\2\u09d1\u09d2\7\u0121\2\2\u09d2\u00d9"+
		"\3\2\2\2\u09d3\u09d5\7\u010e\2\2\u09d4\u09d3\3\2\2\2\u09d4\u09d5\3\2\2"+
		"\2\u09d5\u09d6\3\2\2\2\u09d6\u09f0\7\u011d\2\2\u09d7\u09d9\7\u010e\2\2"+
		"\u09d8\u09d7\3\2\2\2\u09d8\u09d9\3\2\2\2\u09d9\u09da\3\2\2\2\u09da\u09f0"+
		"\7\u011c\2\2\u09db\u09dd\7\u010e\2\2\u09dc\u09db\3\2\2\2\u09dc\u09dd\3"+
		"\2\2\2\u09dd\u09de\3\2\2\2\u09de\u09f0\7\u0119\2\2\u09df\u09e1\7\u010e"+
		"\2\2\u09e0\u09df\3\2\2\2\u09e0\u09e1\3\2\2\2\u09e1\u09e2\3\2\2\2\u09e2"+
		"\u09f0\7\u011a\2\2\u09e3\u09e5\7\u010e\2\2\u09e4\u09e3\3\2\2\2\u09e4\u09e5"+
		"\3\2\2\2\u09e5\u09e6\3\2\2\2\u09e6\u09f0\7\u011b\2\2\u09e7\u09e9\7\u010e"+
		"\2\2\u09e8\u09e7\3\2\2\2\u09e8\u09e9\3\2\2\2\u09e9\u09ea\3\2\2\2\u09ea"+
		"\u09f0\7\u011e\2\2\u09eb\u09ed\7\u010e\2\2\u09ec\u09eb\3\2\2\2\u09ec\u09ed"+
		"\3\2\2\2\u09ed\u09ee\3\2\2\2\u09ee\u09f0\7\u011f\2\2\u09ef\u09d4\3\2\2"+
		"\2\u09ef\u09d8\3\2\2\2\u09ef\u09dc\3\2\2\2\u09ef\u09e0\3\2\2\2\u09ef\u09e4"+
		"\3\2\2\2\u09ef\u09e8\3\2\2\2\u09ef\u09ec\3\2\2\2\u09f0\u00db\3\2\2\2\u09f1"+
		"\u09f2\t\37\2\2\u09f2\u00dd\3\2\2\2\u09f3\u09f4\t \2\2\u09f4\u00df\3\2"+
		"\2\2\u09f5\u09f6\t!\2\2\u09f6\u00e1\3\2\2\2\u0154\u00f9\u0103\u0108\u010b"+
		"\u0110\u011d\u0121\u0126\u0129\u0130\u013e\u0140\u0144\u0147\u014e\u015b"+
		"\u0164\u0166\u016a\u016d\u0174\u017a\u0180\u018b\u01ab\u01b3\u01b7\u01bc"+
		"\u01c2\u01ca\u01d0\u01dd\u01e2\u01eb\u01f0\u0200\u0207\u020b\u0213\u021a"+
		"\u0221\u0230\u0234\u023a\u0240\u0243\u0246\u024c\u0250\u0254\u0259\u025d"+
		"\u0265\u0268\u0271\u0276\u027c\u0283\u0286\u028c\u0297\u029a\u029e\u02a3"+
		"\u02a8\u02af\u02b2\u02b5\u02bc\u02c1\u02ca\u02d2\u02d8\u02dc\u02e0\u02e4"+
		"\u02e6\u02ef\u02f5\u02fb\u02fe\u0302\u0305\u0309\u0314\u0317\u031b\u0321"+
		"\u0324\u0327\u032d\u0335\u033a\u0340\u0346\u0351\u0359\u0360\u0368\u036b"+
		"\u0373\u0377\u037e\u03f2\u03fa\u0402\u040b\u0417\u041b\u041e\u0424\u042e"+
		"\u043a\u043f\u0445\u0451\u0453\u0458\u045c\u0461\u0466\u0469\u046e\u0472"+
		"\u0477\u0479\u047d\u0486\u048e\u0497\u049e\u04a7\u04ac\u04bb\u04c2\u04c5"+
		"\u04cc\u04d0\u04d6\u04de\u04e9\u04f4\u04fb\u0501\u050e\u0510\u0519\u051b"+
		"\u0524\u0527\u0530\u0533\u053c\u053f\u0548\u054b\u054e\u0553\u0555\u0564"+
		"\u056b\u0572\u0575\u0577\u0582\u0586\u058a\u0596\u0599\u059d\u05a7\u05ab"+
		"\u05ad\u05b0\u05b4\u05b7\u05bb\u05c1\u05c5\u05c9\u05ce\u05d1\u05d3\u05d8"+
		"\u05dd\u05e0\u05e4\u05e7\u05e9\u05ee\u05f3\u0600\u0605\u060d\u0613\u0617"+
		"\u0620\u062f\u0634\u0640\u0645\u064d\u0650\u0654\u0662\u066f\u0674\u0678"+
		"\u067b\u0680\u0689\u068c\u0691\u0698\u069b\u06a3\u06aa\u06b1\u06b4\u06b9"+
		"\u06bc\u06c1\u06c5\u06c8\u06cb\u06d1\u06d6\u06db\u06ed\u06ef\u06f2\u06fd"+
		"\u0706\u070d\u0715\u071d\u0721\u0729\u0731\u0737\u073f\u074b\u074e\u0754"+
		"\u0758\u075a\u0763\u076f\u0771\u0778\u077f\u0785\u078b\u078d\u0794\u079a"+
		"\u07a1\u07a7\u07ab\u07ad\u07b4\u07bd\u07ca\u07cf\u07d3\u07e1\u07e3\u07eb"+
		"\u07ed\u07f1\u07f9\u0802\u0808\u0810\u0816\u081b\u0820\u0826\u083b\u083d"+
		"\u0846\u084a\u0853\u0857\u0869\u086c\u0874\u087d\u0894\u089f\u08a6\u08a9"+
		"\u08ae\u08c1\u08d2\u08dc\u08de\u08eb\u08ed\u08f9\u08fe\u0905\u0908\u090e"+
		"\u0911\u0915\u091c\u092d\u0931\u093a\u093e\u0940\u0947\u094e\u0955\u095d"+
		"\u096a\u097e\u0988\u098b\u0994\u0997\u0999\u099c\u099f\u09b1\u09ba\u09c1"+
		"\u09c7\u09cf\u09d4\u09d8\u09dc\u09e0\u09e4\u09e8\u09ec\u09ef";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}