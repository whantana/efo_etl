import click


@click.group()
@click.option('-d', '--db-endpoint', 'db_endpoint', required=True)
@click.option('--bulk/--incremental', default=False)
@click.option('-c', '--cutoff-term_count', 'cutoff_term_count', required=False, type=int)
@click.option('-w', '--worker-count', 'worker_count', required=False, type=int, default=1, show_default=True)
@click.option('-s', '--page-size', 'page_size', required=False, type=int, default=20, show_default=True)
@click.pass_context
def cli(ctx, db_endpoint, cutoff_term_count, page_size, bulk, worker_count):
    ctx.ensure_object(dict)
    ctx.obj['db_endpoint'] = db_endpoint
    ctx.obj['cutoff_term_count'] = cutoff_term_count
    ctx.obj['page_size'] = page_size
    ctx.obj['bulk'] = bulk
    ctx.obj['worker_count'] = worker_count


@cli.command()
@click.pass_context
def naive(ctx):
    # do the naive traversal of the ontology
    from efo_etl.efo_etl import naive_traversal
    naive_traversal(ctx)


@cli.command()
@click.pass_context
def breadth_first(ctx):
    # do the dfs traversal of the ontology
    from efo_etl.efo_etl import breadth_first_traversal
    breadth_first_traversal(ctx)


@cli.command()
@click.pass_context
def depth_first(ctx):
    # do the dfs traversal of the ontology
    from efo_etl.efo_etl import depth_first_traversal
    depth_first_traversal(ctx)


if __name__ == '__main__':
    import time
    tic = time.perf_counter()
    cli(obj={})
    toc = time.perf_counter()
    tdiff = toc - tic
    click.echo(f"Process took {tdiff/60:0.0f} minutes {tdiff%60:0.0f} seconds")

