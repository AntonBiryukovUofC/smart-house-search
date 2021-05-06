import panel as pn
import param


class Healthcheck(param.Parameterized):
    title = pn.pane.Markdown("# Health check")

    def panel(self):
        result = self.title
        return result


res = Healthcheck(name="health").panel()
res.servable()