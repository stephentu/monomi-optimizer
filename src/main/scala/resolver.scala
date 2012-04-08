trait Resolver extends Traversals {
  def resolve(stmt: SelectStmt, schema: Map[String, Relation]): SelectStmt = {
    // init contexts first
    
    val n = {
      def gen(x: Node, ctx: Context): Context = x match {
        case SelectStmt(_, _, _, _, _, _, _) => new Context(Option(ctx))
        case _ => ctx 
      }
      def trfm(x: Node, ctx: Context): Node = x.copyWithContext(ctx)
      traverseWithContext[Context](stmt, null)(gen)(trfm)
    }

    traverse(n) {
      case s @ SelectStmt(_, r, _, _, _, _, ctx) => s
      case f @ FieldIdent(qual, name, sym, ctx) =>
        assert(sym eq null)
        f
      case e => e
    }.asInstanceOf[SelectStmt]
  }
}
