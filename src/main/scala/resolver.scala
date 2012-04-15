trait Resolver extends Transformers with Traversals {
  case class ResolutionException(msg: String) extends RuntimeException(msg)

  private var ctr = 0
  private def anonName = {
    val r = "anon$" + ctr
    ctr += 1
    r
  }

  def resolve(stmt: SelectStmt, schema: Definitions): SelectStmt = {
    // init contexts 
    val n1 = topDownTransformationWithParent(stmt)((parent: Option[Node], child: Node) => child match {
      case s: SelectStmt => 
        parent match {
          case _: SubqueryRelationAST =>
            (Some(s.copyWithContext(new Context(Left(schema)))), true)
          case _ =>
            (Some(s.copyWithContext(new Context(parent.map(c => Right(c.ctx)).getOrElse(Left(schema))))), true)
        }
      case e => 
        (Some(e.copyWithContext(parent.map(_.ctx).getOrElse(throw new RuntimeException("should have ctx")))), true)
    })

    // build contexts up
    topDownTraversal(n1)(wrapReturnTrue {
      case s @ SelectStmt(projections, relations, _, _, _, _, ctx) => 

        //println(s)

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
            val r = schema.defns.get(name).getOrElse(
              throw ResolutionException("no such table: " + name))

            // add relation to ctx
            ctx.relations += (name0 -> TableRelation(name))

          case SubqueryRelationAST(subquery, alias, ctxInner) =>
            // check name
            val name0 = checkName(alias, None, ctx)

            // add relation to ctx
            ctx.relations += (name0 -> SubqueryRelation(subquery))

          case JoinRelation(left, right, _, _, _) =>
            processRelation(left)
            processRelation(right)

        }

        relations.map(_.foreach(processRelation))

        var seenWildcard = false
        projections.foreach {
          case ExprProj(f @ FieldIdent(qual, name, _, _), alias, _) =>
            ctx.projections += NamedProjection(alias.getOrElse(name), f)
          case ExprProj(expr, alias, _) => 
            ctx.projections += NamedProjection(alias.getOrElse("$unknown$"), expr)
          case StarProj(_) if !seenWildcard =>
            ctx.projections += WildcardProjection
            seenWildcard = true
          case _ =>
        }

      case _ => (None, true)
    })

    // resolve field idents
    topDownTransformation(n1) {
      case f @ FieldIdent(qual, name, _, ctx) =>
        val cols = ctx.lookupColumn(qual, name)
        if (cols.isEmpty) throw new ResolutionException("no such column: " + f.sql)
        if (cols.size > 1) throw new ResolutionException("ambiguous reference: " + f.sql)
        (Some(FieldIdent(qual, name, cols.head, ctx)), true)
      case _ => (None, true)
    }.asInstanceOf[SelectStmt]
  }
}
