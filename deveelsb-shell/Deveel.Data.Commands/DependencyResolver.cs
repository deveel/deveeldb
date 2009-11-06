using System;
using System.Collections;
using System.Text;

using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	/**
	 * Resolves dependencies between a given set of tables in respect to their foreign keys.<br>
	 * Created on: Sep 20, 2004<br>
	 * 
	 * @author <a href="mailto:martin.grotzke@javakaffee.de">Martin Grotzke</a>
	 * @version $Id: DependencyResolver.java,v 1.3 2005-06-18 04:58:13 hzeller Exp $
	 */
	class DependencyResolver {

		private readonly IEnumerator _tableIter;
		private IList cyclicDependencies/*<List<Table>>*/;

		/**
		 * @param tableIter An <code>Iterator</code> over <code>Table</code>s.
		 */
		public DependencyResolver(IEnumerator/*<Table>*/ tableIter) {
			_tableIter = tableIter;
		}

		/**
		 * @param tables A <code>Set</code> of <code>Table</code> objects.
		 */
		public DependencyResolver(IList/*<Table>*/ tables) {
			_tableIter = (tables != null) ? tables.GetEnumerator() : null;
		}

		/**
		 * @return
		 * 
		 */
		public ResolverResult sortTables() {
			Hashtable resolved = new Hashtable();
			IDictionary unresolved = null;

			// first run: separate tables with and without dependencies
			while (_tableIter.MoveNext()) {
				Shell.Table t = (Shell.Table)_tableIter.Current;
				if (t == null) {
					continue;
				}

				ICollection fks = t.ForeignKeys;

				// no dependency / foreign key?
				if (fks == null) {
					// System.out.println( "[sortTables] put " + t + " to resolved." );
					resolved.Add(t.Name, t);
				} else {
					// dependency fulfilled?
					bool nodep = true;
					IEnumerator iter2 = fks.GetEnumerator();
					while (iter2.MoveNext() && nodep) {
						ForeignKey fk = (ForeignKey)iter2.Current;
						if (!resolved.ContainsKey(fk.ReferencedTable)) {
							nodep = false;
						}
					}

					if (nodep) {
						// System.out.println( "[sortTables] put " + t + " to resolved." );
						resolved.Add(t.Name, t);
					} else {
						if (unresolved == null)
							unresolved = new Hashtable();
						// System.out.println( "[sortTables] put " + t + " to unresolved." );
						unresolved.Add(t.Name, t);
					}
				}
			}

			// second run: we check remaining deps
			if (unresolved != null) {
				IEnumerator iter = unresolved.Values.GetEnumerator();
				while (iter.MoveNext()) {
					Shell.Table t = (Shell.Table)iter.Current;
					resolveDep(t, null, resolved, unresolved);
				}
			}

			// do we need a second run?
			// unresolved = cleanUnresolved( resolved, unresolved );

			// add all unresolved/conflicting tables to the resulting list
			IList result = new ArrayList(resolved.Values);
			if (unresolved != null) {
				IEnumerator iter = unresolved.Values.GetEnumerator();
				while (iter.MoveNext()) {
					Object table = iter.Current;
					if (!result.Contains(table))
						result.Add(table);
				}
			}

			return new ResolverResult(result, cyclicDependencies);
		}

		/**
		 * @return
		 * 
		 */
		/* Martin: needed ?
		private Set restructureDeps() {
			Set deps = null;
			if ( cyclicDependencies != null ) {
				deps = new HashSet();
				Iterator iter = cyclicDependencies.iterator();
				while ( iter.hasNext() )
					deps.add( ((ListMap)iter.next()).valuesList() );
			}
			return deps;
		}
	*/
		/**
		 * @param resolved
		 * @param unresolved
		 * @return A Map which contains all yet unresolved Tables mapped to their names.
		 */
		/* Martin: needed ?
		private Map cleanUnresolved( Map resolved, Map unresolved ) {
			Map result = null;
        
			if ( unresolved != null ) {
				Iterator iter = unresolved.keySet().iterator();
				while ( iter.hasNext() ) {
					// type element = (type) iter.next();
                
				}
			}
        
			return null;
		}
	*/
		/**
		 * @param t
		 * @param cyclePath	The path of tables which have cyclic dependencies
		 * @param resolved
		 * @param unresolved
		 */
		private void resolveDep(Shell.Table t, IList/*<Table>*/ cyclePath, IDictionary resolved, IDictionary unresolved) {

			// System.out.println( "[resolveDep] >>> Starting for t: " + t + " and cyclePath: " + cyclePath );

			// if the current table is no more in the unresolved collection
			if (t == null || resolved.Contains(t.Name))
				return;

			bool nodep = false;
			bool firstrun = true;
			ICollection fks = t.ForeignKeys;
			IEnumerator iter = fks.GetEnumerator();
			while (iter.MoveNext()) {
				ForeignKey fk = (ForeignKey)iter.Current;

				// Console.Out.WriteLine( "[resolveDep] FK -> " + fk.PkTable + ": " + resolved.ContainsKey( fk.PkTable ) );
				if (!resolved.Contains(fk.ReferencedTable)) {

					Shell.Table inner = (Shell.Table)unresolved[fk.ReferencedTable];

					// if there's yet a cycle with the two tables inner following t
					// then proceed to the next FK and ignore this potential cycle
					if (duplicateCycle(t, inner))
						continue;

					if (cyclePath != null && cyclePath.Contains(inner)) {

						cyclePath.Add(t);

						// create a new list for the detected cycle to add to the
						// cyclicDeps, the former one (cyclePath) is used further on
						ArrayList cycle = new ArrayList(cyclePath);
						cycle.Add(inner);
						if (cyclicDependencies == null)
							cyclicDependencies = new ArrayList();
						// System.out.println("[resolveDep] +++ Putting cyclePath: " + cycle );
						cyclicDependencies.Add(cycle);
						continue;

					} else {
						if (cyclePath == null) {
							// System.out.println("[resolveDep] Starting cyclePath with: " + t);
							cyclePath = new ArrayList();
						}
						cyclePath.Add(t);
					}

					resolveDep(inner, cyclePath, resolved, unresolved);

					if (resolved.Contains(fk.ReferencedTable)) {
						nodep = (firstrun || nodep) && true;
						firstrun = false;
					}
				} else {
					nodep = (firstrun || nodep) && true;
					firstrun = false;
				}
			}

			if (nodep && !resolved.Contains(t.Name)) {
				// System.out.println( "[resolveDep] put " + t + " to resolved." );
				resolved.Add(t.Name, t);
			}

		}

		/**
		 * Tests if there's yet a cycle (stored in cyclicDependencies) with
		 * the given tables t and inner, whith inner following t.
		 * @param t
		 * @param inner
		 * @return
		 */
		private bool duplicateCycle(Shell.Table t, Shell.Table inner) {
			bool result = false;
			if (cyclicDependencies != null) {
				IEnumerator iter = cyclicDependencies.GetEnumerator();
				while (iter.MoveNext() && !result) {
					IList path = (IList)iter.Current;
					if (path.Contains(t)) {
						int tIdx = path.IndexOf(t);
						if (path.Count > tIdx + 1 && inner.Equals(path[tIdx + 1])) {
							result = true;
						}
					}
				}
			}
			return result;
		}

		public class ResolverResult {
			private readonly IList/*<Table>*/ _tables;
			private readonly IList/*<List<Table>>*/ _cyclicDependencies;
			public ResolverResult(IList tables, IList cyclicDependencies) {
				_tables = tables;
				_cyclicDependencies = cyclicDependencies;
			}
			/**
			 * @return Returns the cyclicDependencies: a <code>Set</code> holding
			 * <code>List</code>s of <code>CycleEntry</code> objects, where each list
			 * represents the path of a cyclic dependency.
			 */
			public IList/*<List<Table>>*/ getCyclicDependencies() {
				return _cyclicDependencies;
			}
			/**
			 * @return Returns the tables.
			 */
			public IList/*<Table>*/ getTables() {
				return _tables;
			}
		}

		public class CycleEntry {
			private Shell.Table _table;
			private ForeignKey _fk;
			public CycleEntry(Shell.Table table, ForeignKey fk) {
				_table = table;
				_fk = fk;
			}
			/**
			 * @return Returns the fk.
			 */
			public ForeignKey getFk() {
				return _fk;
			}
			/**
			 * @return Returns the table.
			 */
			public Shell.Table getTable() {
				return _table;
			}
			public override String ToString() {
				StringBuilder sb = new StringBuilder("CycleEntry [");
				sb.Append("table: ").Append(_table.Name);
				sb.Append(", fk: ").Append(_fk.ToString());
				sb.Append("]");
				return sb.ToString();
			}

			public override bool Equals(Object other) {
				bool result = false;
				if (other == this)
					result = true;
				else if (other is CycleEntry) {
					CycleEntry ce = (CycleEntry)other;
					if (_table == null && ce.getTable() == null && 
						_fk == null && ce.getFk() == null)
						result = true;
					else if (_table.Equals(ce.getTable()) && 
						_fk.Equals(ce.getFk()))
						result = true;
				}
				return result;
			}
		}
	}
}