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

  def generatePlainBenchmark(baseFolder: File, stmts: Seq[SelectStmt]): Unit = {
    baseFolder.mkdirs()

    def makeProgram() = {
      val progFile = new File(baseFolder, "program.cc.generated")
      val cg = new CodeGenerator(progFile)

      cg.println("// WARNING: This is an auto-generated file")
      cg.println("#include \"db_config.h\"")

      cg.println("#include <cassert>")
      cg.println("#include <cstdlib>")
      cg.println("#include <iostream>")

      cg.println("#include <edb/ConnectNew.hh>")
      cg.println("#include <util/util.hh>")

      stmts.zipWithIndex.foreach { case (stmt, idx) =>
        assert(!stmt.projections.isEmpty)
        cg.blockBegin("static void query_%d(ConnectNew& conn) {".format(idx))
          cg.println("DBResultNew* dbres;")
          cg.print("conn.execute(")
          cg.printStr(stmt.sqlFromDialect(PostgresDialect))
          cg.println(", dbres);")
          cg.println("ResType res = dbres->unpack();")
          cg.println("assert(res.ok);")
          cg.blockBegin("for (auto &row : res.rows) {")
            (0 until stmt.projections.size).foreach { c =>
              cg.println("std::cout << row[%d].data;".format(c))
              if ((c + 1) == stmt.projections.size) {
                cg.println("std::cout << std::endl;")
              } else {
                cg.println("std::cout << \"|\";")
              }
            }
          cg.blockEnd("}")
          cg.println("std::cout << \"(\" << res.rows.size() << \" rows)\" << std::endl;")
          cg.println("delete dbres;")
        cg.blockEnd("}")
      }

      cg.blockBegin("int main(int argc, char **argv) {")

        cg.blockBegin("if (argc != 2) {")
          cg.println("std::cerr << \"[Usage]: \" << argv[0] << \" [query num]\" << std::endl;")
          cg.println("return 1;")
        cg.blockEnd("}")

        cg.println("int q = atoi(argv[1]);")
        cg.println("PGConnect pg(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_PORT);")

        cg.blockBegin("switch (q) {")
          (0 until stmts.size).foreach { i =>
            cg.println("case %d: query_%d(pg); break;".format(i, i))
          }
          cg.println("default: assert(false);")
        cg.blockEnd("}")

        cg.println("return 0;")

      cg.blockEnd("}")
    }

    def makeConfig() = {
      val configFile = new File(baseFolder, "db_config.h.sample")
      val cg = new CodeGenerator(configFile)
      cg.println("// copy this file to db_config.h, filling in the appropriate values")
      cg.println("#define DB_HOSTNAME \"localhost\"")
      cg.println("#define DB_USERNAME \"user\"")
      cg.println("#define DB_PASSWORD \"pass\"")
      cg.println("#define DB_DATABASE \"db\"")
      cg.println("#define DB_PORT     5432")
    }

    def makeMakefile() = {
      val makefile = new File(baseFolder, "Makefrag.sample")
      val cg = new CodeGenerator(makefile)
      val dirname = baseFolder.getName
      val dirnameCaps = dirname.toUpperCase
      val dirnameCapsSanitized = dirnameCaps.replace('-', '_')

      cg.println("OBJDIRS += " + dirname)
      cg.println(dirnameCapsSanitized + "PROGS := program")
      cg.println(dirnameCapsSanitized + "PROGOBJS := $(patsubst %,$(OBJDIR)/" + dirname + "/%,$(" + dirnameCapsSanitized + "PROGS))")
      cg.println("all: $(" + dirnameCapsSanitized + "PROGOBJS)")
      cg.println("$(" + dirnameCapsSanitized + "PROGOBJS): %: %.o $(OBJDIR)/libcryptdb.so $(OBJDIR)/libedbutil.so")
      cg.println("\t$(CXX) $< -o $@ -ledbparser  $(LDFLAGS) -ledbutil -lcryptdb -lmysqlclient -ltbb -lgmp -lpq")
    }

    makeProgram()
    makeConfig()
    makeMakefile()
  }

  def generate(baseFolder: File, plans: Seq[(PlanNode, CodeGenContext)]): Unit = {
    baseFolder.mkdirs()

    def makeProgram() = {
      val progFile = new File(baseFolder, "program.cc.generated")
      val cg = new CodeGenerator(progFile)
      cg.println("// WARNING: This is an auto-generated file")
      cg.println("#include \"db_config.h\"")

      cg.println("#include <cassert>")
      cg.println("#include <cstdlib>")
      cg.println("#include <iostream>")

      cg.println("#include <crypto-old/CryptoManager.hh>")
      cg.println("#include <crypto/paillier.hh>")
      cg.println("#include <edb/ConnectNew.hh>")
      cg.println("#include <execution/encryption.hh>")
      cg.println("#include <execution/context.hh>")
      cg.println("#include <execution/operator_types.hh>")
      cg.println("#include <execution/eval_nodes.hh>")
      cg.println("#include <execution/query_cache.hh>")
      cg.println("#include <execution/commandline.hh>")
      cg.println("#include <util/util.hh>")

      cg.blockBegin("static inline size_t _FP(size_t i) {")
        cg.println("#ifdef ALL_SAME_KEY")
        cg.println("return 0;")
        cg.println("#else")
        cg.println("return i;")
        cg.println("#endif /* ALL_SAME_KEY */")
      cg.blockEnd("}")

      cg.blockBegin("static inline bool _FJ(bool join) {")
        cg.println("#ifdef ALL_SAME_KEY")
        cg.println("return false;")
        cg.println("#else")
        cg.println("return join;")
        cg.println("#endif /* ALL_SAME_KEY */")
      cg.blockEnd("}")

      plans.foreach { case (p, ctx) => p.emitCPPHelpers(cg, ctx) }

      plans.zipWithIndex.foreach { case ((p, ctx), idx) =>
        cg.blockBegin("static void query_%d(exec_context& ctx) {".format(idx))

          cg.print("physical_operator* op = ")
          p.emitCPP(cg, ctx)
          cg.println(";")

          cg.println("op->open(ctx);")
          cg.blockBegin("while (op->has_more(ctx)) {")

            cg.println("physical_operator::db_tuple_vec v;")
            cg.println("op->next(ctx, v);")
            cg.println("physical_operator::print_tuples(v);")

          cg.blockEnd("}")
          cg.println("op->close(ctx);")
          cg.println("delete op;")

        cg.blockEnd("}")
      }

      cg.blockBegin("int main(int argc, char **argv) {")

        cg.println("command_line_opts opts(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_PORT);")
        cg.println("command_line_opts::parse_options(argc, argv, opts);")
        cg.println("std::cerr << opts << std::endl;")
        cg.blockBegin("if ((optind + 1) != argc) {")
          cg.println("std::cerr << \"[Usage]: \" << argv[0] << \" [options] [query num]\" << std::endl;")
          cg.println("return 1;")
        cg.blockEnd("}")

        cg.println("int q = atoi(argv[optind]);")

        cg.println("CryptoManager cm(\"12345\");")
        cg.println("crypto_manager_stub cm_stub(&cm, CRYPTO_USE_OLD_OPE);")
        cg.println("PGConnect pg(opts.db_hostname, opts.db_username, opts.db_passwd, opts.db_database, opts.db_port, true);")
        cg.println("paillier_cache pp_cache;")
        cg.println("query_cache cache;")
        cg.println("exec_context ctx(&pg, &cm_stub, &pp_cache, NULL, &cache);")

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
      val configFile = new File(baseFolder, "db_config.h.sample")
      val cg = new CodeGenerator(configFile)
      cg.println("// copy this file to db_config.h, filling in the appropriate values")
      cg.println("#define DB_HOSTNAME \"localhost\"")
      cg.println("#define DB_USERNAME \"user\"")
      cg.println("#define DB_PASSWORD \"pass\"")
      cg.println("#define DB_DATABASE \"db\"")
      cg.println("#define DB_PORT     5432")

      cg.println("")

      cg.println("#define CRYPTO_USE_OLD_OPE false")
    }

    def makeMakefile() = {
      val makefile = new File(baseFolder, "Makefrag.sample")
      val cg = new CodeGenerator(makefile)
      val dirname = baseFolder.getName
      val dirnameCaps = dirname.toUpperCase
      val dirnameCapsSanitized = dirnameCaps.replace('-', '_')

      cg.println("OBJDIRS += " + dirname)
      cg.println(dirnameCapsSanitized + "PROGS := program")
      cg.println(dirnameCapsSanitized + "PROGOBJS := $(patsubst %,$(OBJDIR)/" + dirname + "/%,$(" + dirnameCapsSanitized + "PROGS))")
      cg.println("all: $(" + dirnameCapsSanitized + "PROGOBJS)")
      cg.println("$(" + dirnameCapsSanitized + "PROGOBJS): %: %.o $(OBJDIR)/libcryptdb.so $(OBJDIR)/libedbparser.so $(OBJDIR)/libedbutil.so $(OBJDIR)/libexecution.so")
      cg.println("\t$(CXX) $< -o $@ -ledbparser  $(LDFLAGS) -ledbutil -lcryptdb -ledbcrypto -ledbcrypto2 -lexecution -lmysqlclient -ltbb -lgmp -lpq")
    }

    makeProgram()
    makeConfig()
    makeMakefile()
  }
}
