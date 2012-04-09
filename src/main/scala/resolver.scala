trait Resolver extends Traversals {
  case class ResolutionException(msg: String) extends RuntimeException(msg)

  private var ctr = 0
  private def anonName = {
    val r = "anon$" + ctr
    ctr += 1
    r
  }

  def resolve(stmt: SelectStmt, schema: Map[String, TableRelation]): SelectStmt = {

    // init contexts 
    val n = {
      def gen(x: Node, ctx: Context): Context = x match {
        case SelectStmt(_, _, _, _, _, _, _) => new Context(Option(ctx))
        case _ => ctx 
      }
      def trfm(x: Node, ctx: Context): Node = x.copyWithContext(ctx)
      traverseWithContext[Context](stmt, null)(gen)(trfm)
    }

    // build contexts up
    val n1 = traverse(n) {
      case s @ SelectStmt(projections, relations, _, _, _, _, ctx) => 

        println(s)

        def checkName(name: String, alias: Option[String], ctx: Context): String = {
          val name0 = alias.getOrElse(name)
          if (ctx.relations.contains(name0)) {
            throw ResolutionException("relation " + name0 + " is referenced twice w/ same alias")
          }
          name0
        }

        def processRelation(r: SqlRelation): Unit = r match {
          case TableRelationAST(name, alias, _) => 

            println("processing: " + name)
            println(ctx.relations)

            // check name
            val name0 = checkName(name, alias, ctx)

            // check valid table 
            val r = schema.get(name).getOrElse(
              throw ResolutionException("no such table: " + name))

            // add relation to ctx
            ctx.relations += (name0 -> r)

          case SubqueryRelationAST(subquery, alias, ctxInner) =>
            // check name
            val name0 = checkName(alias, None, ctx)

            // add relation to ctx
            ctx.relations += ((name0, SubqueryRelation(name0, subquery.ctx.projections.toSeq, subquery)))

          case JoinRelation(left, right, _, _, _) =>
            processRelation(left)
            processRelation(right)

        }

        relations.map(_.foreach(processRelation))

        projections.foreach {
          case ExprProj(FieldIdent(qual, name, _, _), alias, _) =>
            ctx.projections += 
            ctx.lookupColumn(qual, name).map(x => alias.map(a => AliasedColumn(a, x)).getOrElse(x)).getOrElse(
                throw ResolutionException("no such field: " + name))
          case ExprProj(expr, alias, _) => 
            ctx.projections +=
              ExprColumn(alias.getOrElse(anonName), expr)
          case StarProj(_) =>
            ctx.relations.foreach {
              case (name, relation) => 
                ctx.projections ++= relation.columns
            }
        }

        s
      case e => e
    }

    // resolve field idents
    traverse(n1) {
      case FieldIdent(qual, name, _, ctx) =>
        val col = 
          ctx.lookupColumn(qual, name).getOrElse(
            throw ResolutionException("no such field: " + name))
        FieldIdent(qual, name, col, ctx) 
      case e => e
    }.asInstanceOf[SelectStmt]
  }
}
