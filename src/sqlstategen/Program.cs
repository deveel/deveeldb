using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.IO;
using System.Xml;

using Deveel.Configuration;

using Microsoft.CSharp;

namespace sqlstategen {
	class Program {
		private static Options GetOptions() {
			Options options = new Options();
			
			Option option = new Option("i", "input", true, "the input file to read");
			option.IsRequired = true;
			options.AddOption(option);

			option = new Option("f", "format", true, "The format of the input file (default: XML)");
			options.AddOption(option);

			option = new Option("o", "ouput", true, "Ouput file to generate (default: ./SqlState_States.cs)");
			options.AddOption(option);

			options.AddOption("?", "help", false, "Prints the application usage.");

			return options;
		}

		private static void GenerateStatesFileFromXml(string inputFile, string outputFile) {
			if (File.Exists(outputFile))
				File.Delete(outputFile);

			using (StreamWriter output = new StreamWriter(outputFile)) {
				XmlDocument document = new XmlDocument();
				document.Load(inputFile);

				XmlElement rootElement = document.DocumentElement;

				CodeCompileUnit compileUnit = new CodeCompileUnit();
				CodeNamespace nspace = new CodeNamespace("Deveel.Data");
				CodeTypeDeclaration typeDecl = new CodeTypeDeclaration("SqlState");
				typeDecl.IsPartial = true;
				CodeTypeConstructor cctor = new CodeTypeConstructor();

				foreach (XmlNode childNode in rootElement.ChildNodes) {
					if (childNode.NodeType != XmlNodeType.Element)
						continue;

					AddStateFromXml(cctor, (XmlElement) childNode);
				}

				typeDecl.Members.Add(cctor);
				nspace.Types.Add(typeDecl);
				compileUnit.Namespaces.Add(nspace);

				CSharpCodeProvider provider = new CSharpCodeProvider();
				ICodeGenerator generator = provider.CreateGenerator(outputFile);
				CodeGeneratorOptions options = new CodeGeneratorOptions();
				options.IndentString = "\t";
				options.BracingStyle = "B";
				options.BlankLinesBetweenMembers = true;
				generator.GenerateCodeFromCompileUnit(compileUnit, output, options);

				output.Flush();
				output.Close();
			}
		}

		private static void AddStateFromXml(CodeTypeConstructor cctor, XmlElement childNode) {
			string stateClass = childNode.GetAttribute("class");
			string stateSubclass = childNode.GetAttribute("subclass");
			string name = childNode.GetAttribute("name");
			int sqlCode = Int32.Parse(childNode.GetAttribute("code"));

			CodeMethodInvokeExpression addState = new CodeMethodInvokeExpression();
			addState.Method = new CodeMethodReferenceExpression(null, "AddState");
			addState.Parameters.Add(new CodePrimitiveExpression(name));
			addState.Parameters.Add(new CodePrimitiveExpression(sqlCode));
			addState.Parameters.Add(new CodePrimitiveExpression(stateClass));
			addState.Parameters.Add(new CodePrimitiveExpression(stateSubclass));

			cctor.Statements.Add(addState);
		}

		[STAThread]
		static void Main(string[] args) {
			Options options = GetOptions();
			ICommandLineParser parser = new GnuParser(options);

			CommandLine commandLine = parser.Parse(args);

			string inputFile = commandLine.GetOptionValue("i");
			if (!File.Exists(inputFile)) {
				Console.Error.WriteLine("Input file '{0}' not found!", inputFile);
				Environment.Exit(1);
			}

			string format = commandLine.GetOptionValue("f", "xml");
			if (!String.Equals(format, "xml", StringComparison.InvariantCultureIgnoreCase)) {
				Console.Error.WriteLine("Input format {0} not supported yet.", format);
				Environment.Exit(1);
			}

			string outputFile = commandLine.GetOptionValue("o", "SqlState_States.cs");

			try {
				GenerateStatesFileFromXml(inputFile, outputFile);
			} catch (Exception e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				Environment.Exit(2);
			}
		}
	}
}
