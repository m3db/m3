+++
title = "Home"
date = 2020-04-01T19:26:56-04:00
weight = 4
chapter = false
+++

{{< tabs name="tab_with_md" >}}
{{% tab name="Markdown" %}}
This is **some markdown.**
```bash
code
```
{{% /tab %}}
{{< tab name="HTML" >}}

HTML

{{< /tab >}}
{{< tab name="JSON" >}}

Include code from elsewhere.

{{% codeinclude file="/static/podtemplate.json" language="json" %}}

{{< /tab >}}
{{< /tabs >}}

{{< glossary_tooltip text="test" term_id="test" >}}

```go {hl_lines=[2]}
func GetTitleFunc(style string) func(s string) string {
  switch strings.ToLower(style) {
  case "go":
    return strings.Title
  case "chicago":
    return transform.NewTitleConverter(transform.ChicagoStyle)
  default:
    return transform.NewTitleConverter(transform.APStyle)
  }

```