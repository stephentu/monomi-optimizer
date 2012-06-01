package edu.mit.cryptdb

import java.io._

class CodeGenerator(outputFile: File) extends PrettyPrinters {

  private val _idGen = new NameGenerator("id")

  private val pw = new PrintWriter(new FileOutputStream(outputFile), true)

  private var _indent = 0

  private var _didIndent = false

  def blockBegin(l: String): Unit = {
    printIndent()
    pw.println(l)
    _indent += 1
    _didIndent = false
  }

  def blockEnd(l: String): Unit = {
    assert(_indent > 0)
    _indent -= 1
    printIndent()
    pw.println(l)
    _didIndent = false
  }

  def println(l: String): Unit = { 
    printIndent() 
    pw.println(l)
    _didIndent = false
  }

  def print(l: String): Unit = { 
    printIndent()
    pw.print(l) 
  }

  def printStr(l: String): Unit = print(quoteDbl(l))

  def uniqueId(): String = _idGen.uniqueId()

  private def printIndent(): Unit = {
    if (!_didIndent) {
      pw.print("  " * _indent)
      _didIndent = true
    }
  }
}

trait ProgramGenerator {

  def generate(baseFolder: File, plans: Seq[PlanNode]): Unit = {
    baseFolder.mkdirs()

    def makeProgram() = {
      val progFile = new File(baseFolder, "program.cc")
      val cg = new CodeGenerator(progFile)
      cg.println("#include \"db_config.h\"")
      
      cg.println("#include <cassert>")
      cg.println("#include <cstdlib>")
      cg.println("#include <iostream>")

      cg.println("#include <crypto-old/CryptoManager.hh>")
      cg.println("#include <edb/ConnectNew.hh>")
      cg.println("#include <execution/encryption.hh>")
      cg.println("#include <execution/context.hh>")
      cg.println("#include <execution/operator_types.hh>")

      plans.foreach(_.emitCPPHelpers(cg))

      plans.zipWithIndex.foreach { case (p, idx) =>
        cg.blockBegin("static void query_%d(exec_context& ctx) {".format(idx))

          cg.print("physical_operator* op = ")
          p.emitCPP(cg)
          cg.println(";")

          cg.println("op->open(ctx);")
          cg.blockBegin("while (op->has_next(ctx)) {")
            
            cg.println("db_tuple_vec v;")
            cg.println("op->next(ctx, v);")
            cg.println("//TODO: print v")

          cg.blockEnd("}")
          cg.println("op->close(ctx);")
          cg.println("delete op;")

        cg.blockEnd("}")
      }

      cg.blockBegin("int main(int argc, char **argv) {")

        cg.blockBegin("if (argc != 2) {")
          cg.println("std::cerr << \"[Usage]: \" << argv[0] << \" [query num]\" << std::endl;")
          cg.println("return 1;")
        cg.blockEnd("}")

        cg.println("int q = atoi(argv[1]);")

        cg.println("CryptoManager cm(12345);")
        cg.println("PGConnect pg(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_PORT);")
        cg.println("exec_context ctx(&pg, &cm);")

        cg.blockBegin("switch (q) {")
          (0 until plans.size).foreach { i =>
            cg.println("case %d: query_%d(ctx); break;".format(i, i))
          }
          cg.println("default: assert(false);")
        cg.blockEnd("}")

        cg.println("return 0;")

      cg.blockEnd("}")
    }

    def makeConfig() = {
      val configFile = new File(baseFolder, "db_config.h")
      val cg = new CodeGenerator(configFile)
      cg.println("#define DB_HOSTNAME \"localhost\"")
      cg.println("#define DB_USERNAME \"user\"")
      cg.println("#define DB_PASSWORD \"pass\"")
      cg.println("#define DB_DATABASE \"db\"")
      cg.println("#define DB_PORT     5432")
    }

    def makeMakefile() = {
      val makefile = new File(baseFolder, "Makefile")
      val cg = new CodeGenerator(makefile)

    }

    makeProgram()
    makeConfig()
    makeMakefile()
  }
}
