Build propre. Je vérifie le comportement avec un dry-run sur une arborescence de test :Parfait, tout fonctionne comme attendu. Je teste aussi le mode avec fichier de répertoires :Le programme compile, le dry-run est validé. Voici ce qu'il fait et pourquoi c'est memory-safe :

---

## Architecture & choix techniques

### Zéro saturation mémoire
`filepath.WalkDir` est **entièrement streamé** : il ne charge jamais l'arborescence complète en mémoire. Chaque entrée est traitée puis libérée — que l'arbre ait 1 000 ou 1 000 000 chemins, la mémoire reste plate.

Pour la liste de répertoires (flag `-dirs`), c'est un `bufio.Scanner` qui lit **ligne par ligne** : le fichier peut peser des gigaoctets, seule la ligne courante est en RAM.

### Phase 1 — suppression `.terraform`
Dès qu'un répertoire `.terraform` est trouvé, `os.RemoveAll` le supprime et `fs.SkipDir` est retourné : la marche **ne descend pas** dedans. Les erreurs d'accès sont loguées et la marche continue sans s'arrêter.

### Phase 2 — `terraform init -upgrade`
Le worker pool est borné par un **canal sémaphore** (`sem := make(chan struct{}, N)`). Le point clé : l'acquisition du sémaphore se fait **avant** de lancer la goroutine, pas dedans — ce qui évite de créer des milliers de goroutines bloquées.

```
sem <- struct{}{}   // bloque ici si N workers sont déjà actifs
wg.Add(1)
go func() {
    defer wg.Done()
    defer func() { <-sem }()
    execTerraformInit(...)
}()
```

### Flags disponibles

| Flag | Défaut | Rôle |
|---|---|---|
| `-root` | `/workspaces` | Racine pour la suppression des `.terraform` |
| `-dirs` | _(vide = root)_ | Fichier texte listant les répertoires à scanner |
| `-workers` | `6` | Parallélisme max de `terraform init` |
| `-dry-run` | `false` | Simule sans rien exécuter |
| `-verbose` | `false` | Affiche la sortie complète de chaque init |
