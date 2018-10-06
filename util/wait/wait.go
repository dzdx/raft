package wait

import (
	"sync"
)

type Group struct{
	wg sync.WaitGroup
}

func (g *Group)Wait(){
	g.wg.Wait()
}

func (g *Group)Start(f func()){
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}
