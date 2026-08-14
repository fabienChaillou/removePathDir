17/17 tests passent. Je livre les fichiers :17/17 tests verts. Voici la structure et les décisions de design :

---

## Structure des packages

```shell
cleaner/
  cleaner.go          ← Phase 1 (workflow Temporal A)
  cleaner_test.go     ← 5 tests
initializer/
  initializer.go      ← Phase 2 (workflow Temporal B)
  initializer_test.go ← 12 tests
```

---

## Points clés de conception

### Isolation des phases
Chaque package est indépendant, zéro import croisé. Temporal peut appeler `cleaner.RemoveTerraformDirs` et `initializer.RunTerraformInit` depuis des workflows totalement distincts.

### Mémoire sous contrôle (100k+ UUIDs)
Trois niveaux de streaming :
1. **`UUIDFetcher`** retourne un `<-chan string` — l'API est paginée, un UUID en mémoire à la fois. `SimulatedAPIFetcher(pageSize, totalPages)` simule la pagination avec délai réseau.
2. **`filepath.WalkDir`** est entièrement streamé, jamais de slice de chemins.
3. **Sémaphore canal** `sem <- struct{}{}` **avant** le `go func()` — le pool est borné à `Workers` goroutines actives, pas de milliers de goroutines dormantes.

### Testabilité par injection
`CmdRunner func(dir, name string, args ...string) ([]byte, error)` est injecté dans `Options.RunCommand`. Les tests utilisent `recordingRunner` pour :
- vérifier que `terraform init -upgrade` est bien appelé avec les bons args
- simuler des échecs ciblés par répertoire
- mesurer le parallélisme réel pour valider la borne `Workers`

### Compatibilité Temporal
Le `context.Context` est propagé jusqu'à chaque nœud de `WalkDir` et vérifié avant chaque `exec.Command` — une annulation Temporal (heartbeat timeout, cancel signal) arrête proprement la marche et le pool.
