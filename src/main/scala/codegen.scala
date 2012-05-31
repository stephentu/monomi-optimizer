package edu.mit.cryptdb

import java.io._

class CodeGenerator(outputFile: File) extends PrettyPrinters {

  private val pw = new PrintWriter(outputFile) 

  private var _indent = 0

  def blockBegin(l: String): Unit = {
    pw.println(l)
    _indent += 1
    printIndent()
  }

  def blockEnd(l: String): Unit = {
    assert(_indent > 0)
    pw.println(l)
    _indent -= 1
    printIndent()
  }

  def println(l: String): Unit = { pw.println(l); printIndent() }
  def print(l: String): Unit = { pw.print(l) }

  def printStr(l: String): Unit = { print(quoteDbl(l)) }

  private def printIndent(): Unit = {
    pw.print("  " * _indent)
  }
}
