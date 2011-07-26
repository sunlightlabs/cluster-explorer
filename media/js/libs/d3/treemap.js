var w = 960,
    h = 500,
    color = d3.scale.category20c();

var treemap = d3.layout.treemap()
   .size([w, h])
   .sticky(false)
   .children(function(d) { return d.clusters; })
   .value(function(d) { return d.count; });

var div = d3.select("#chart").append("div")
   .style("position", "relative")
   .style("width", w + "px")
   .style("height", h + "px")
   .style("margin", "0 auto");

function drawIt(someData) {
    var theThings = div.data([someData]).selectAll("div").data(treemap.nodes)
    
    console.log(theThings.enter())
    
    theThings.enter().append("div")
        .attr("class", "cell")
        .style("background", function(d) { return d.count ? color(d.count) : null; })
        .call(cell)
        .text(function(d) { return d.children ? null : d.count; })
    
    theThings.exit().remove();
}

function reDrawIt(someData) {
    
    var theThings = div.data([someData]).selectAll(".cell").data(treemap.nodes);

    //update
    theThings.transition().duration(1500).call(cell).text(function(d) { return d.children ? null : d.count; });
    
    //enter
    theThings.enter().append("div").transition().duration(1500)
        .attr("class", "cell")
        .style("background", function(d) { return d.count ? color(d.count) : null; })
        .call(cell)
        .text(function(d) { return d.children ? null : d.count; })
    
    exit = theThings.exit().transition().duration(1500).style("width",0).remove();
        
}

function cell() {
  this
      .style("left", function(d) { return d.x + "px"; })
      .style("top", function(d) { return d.y + "px"; })
      .style("width", function(d) { return d.dx - 1 + "px"; })
      .style("height", function(d) { return d.dy - 1 + "px"; });
}
