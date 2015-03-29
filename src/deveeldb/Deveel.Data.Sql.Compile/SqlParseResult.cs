using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// The result of a parse of an SQL input
	/// </summary>
	/// <seealso cref="ISqlParser"/>
	[Serializable]
	public sealed class SqlParseResult {
		/// <summary>
		/// Constructs a new <see cref="SqlParseResult"/>.
		/// </summary>
		/// <param name="dialect">The SQL dialect of the input.</param>
		public SqlParseResult(string dialect) {
			if (String.IsNullOrEmpty(dialect))
				throw new ArgumentNullException("dialect");

			Dialect = dialect;
			Errors = new List<SqlParseError>();
		}

		/// <summary>
		/// Gets the name of the SQL dialect of the parser
		/// that generated this result.
		/// </summary>
		public string Dialect { get; private set; }

		/// <summary>
		/// Gets or sets the node that is the root of the parsed
		/// nodes from the input.
		/// </summary>
		/// <remarks>
		/// <para>
		/// If the parser produced any tree from the analysis, this
		/// object will be used to construct commands to interact
		/// with the underlying system.
		/// </para>
		/// <para>
		/// In some cases this value is not set, because of previous
		/// errors during the analysis of an input from the parser.
		/// </para>
		/// </remarks>
		/// <seealso cref="ISqlNode"/>
		/// <seealso cref="ISqlNodeVisitor"/>
		public ISqlNode RootNode { get; set; }

		/// <summary>
		/// Gets a boolean value that indicates if the result has
		/// any root node set.
		/// </summary>
		/// <seealso cref="RootNode"/>
		public bool HasRootNode {
			get { return RootNode != null; }
		}

		/// <summary>
		/// Gets a collection of <see cref="SqlParseError"/> that
		/// were found during the parse of an input.
		/// </summary>
		public ICollection<SqlParseError> Errors { get; private set; }

		/// <summary>
		/// Gets or sets the time the parser took to analyze an input provided.
		/// </summary>
		public TimeSpan ParseTime { get; set; }

		/// <summary>
		/// Gets a boolean value indicating if the result has
		/// any error.
		/// </summary>
		/// <seealso cref="Errors"/>
		/// <seealso cref="SqlParseError"/>
		public bool HasErrors {
			get { return Errors.Count > 0; }
		}
	}
}
