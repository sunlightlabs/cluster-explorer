var w = 1240,
    h = 600,
    color = d3.scale.category20c();

var treemap = d3.layout.treemap()
   .size([w, h])
   .sticky(false)
   .children(function(d) { return d.clusters; })
   .value(function(d) { return d.count; })
   .round(true);

var div = d3.select("#chart").append("div")
   .style("position", "relative")
   .style("width", w + "px")
   .style("height", h + "px")
   .style("margin", "0 auto");

function treemapInnerHtml(params) {
    if (params == 'undefined') params = clusterExplore.params;
    div.selectAll('div')
        .html(function(d, i) {
            var ret = ""
            if (d.children == null) {
                ret += "<a href='/"+params.step+'/'+i+"'>"+d.count+" documents</a>";
                //if (d.dx-1 > 80 || d.dy-1 > 80) {
                    ret += "<p>"+d.docs[0].text+"</p>";
                //}
                return ret;
            }
        });
}

function drawIt(someData, params) {
    //if (hash.substr(-1) != "/") hash = hash+"/";
    
    var theThings = div.data([someData]).selectAll("div").data(treemap.nodes);
    //Update
    theThings
        .transition().duration(1500)
            .call(cell)
    //Enter
    theThings.enter()
        .append("div").attr("class","cell")
        .style("background", function(d) { return d.count ? color(d.count) : null; })
        .transition().duration(1500)
            .call(cell)
    
    //Exit
    theThings.exit()
        .transition().duration(1500)
            .style("width","0px")
            .remove();
    
    treemapInnerHtml(params);
}

function makeFlat() {
    treemap.value(function(d){
        return 1;
    });
    div.selectAll("div").data( treemap.nodes ).call(cell);
    $(this).siblings().attr('disabled','');
    $(this).attr('disabled','disabled');
    treemapInnerHtml();
}

function makeProportional() {
    treemap.value(function(d){
        return d.count;
    });
    div.selectAll("div").data( treemap.nodes ).call(cell);
    $(this).siblings().attr('disabled','');
    $(this).attr('disabled','disabled');
    treemapInnerHtml();
}

function cell() {
  this
      .attr("id", function(d, i){ return "cluster-"+(i-1); })
      .style("left", function(d) { return d.x + "px"; })
      .style("top", function(d) { return d.y + "px"; })
      .style("width", function(d) { return d.dx - 1 + "px"; })
      .style("height", function(d) { return d.dy - 1 + "px"; });
}
// width = d.dx-1
// height = d.dy-1